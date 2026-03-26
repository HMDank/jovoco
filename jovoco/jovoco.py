import reflex as rx
from sqlalchemy import text
from typing import List, Dict, Any
from .utils.database.connection import get_session


class State(rx.State):
    top_products: List[Dict[str, Any]] = []
    product_pairs: List[Dict[str, Any]] = []
    customer_segments: List[Dict[str, Any]] = []
    retention_rate: str = "0.0%"
    is_loading: bool = False

    def fetch_gold_data(self):
        self.is_loading = True
        yield

        with get_session() as session:
            query_1 = text("""
                WITH regional_sales AS (
                    SELECT 
                        s.region AS store_region,
                        p.title AS product_name,
                        SUM(f.revenue) AS total_revenue,
                        DENSE_RANK() OVER (PARTITION BY s.region ORDER BY SUM(f.revenue) DESC) AS sales_rank
                    FROM gold.fact_sales f
                    JOIN gold.dim_product p ON f.product_id = p.product_id
                    JOIN gold.dim_store s ON f.store_id = s.store_id
                    GROUP BY s.region, p.title
                )
                SELECT store_region, product_name, CAST(total_revenue AS FLOAT) as total_revenue
                FROM regional_sales
                WHERE sales_rank <= 5
                ORDER BY store_region ASC, total_revenue DESC
            """)
            self.top_products = [
                dict(row) for row in session.execute(query_1).mappings().all()
            ]

            query_2 = text("""
                WITH customer_rev AS (
                    SELECT 
                        customer_id,
                        SUM(revenue) AS total_spending,
                        NTILE(3) OVER (ORDER BY SUM(revenue) DESC) AS tier
                    FROM gold.fact_sales
                    GROUP BY customer_id
                )
                SELECT 
                    c.name AS customer_full_name,
                    CAST(cr.total_spending AS FLOAT) as total_spending,
                    CASE 
                        WHEN cr.tier = 1 THEN 'High Value'
                        WHEN cr.tier = 2 THEN 'Mid Value'
                        ELSE 'Low Value'
                    END AS value_segment
                FROM customer_rev cr
                JOIN gold.dim_customer c ON cr.customer_id = c.customer_id
                ORDER BY total_spending DESC LIMIT 15
            """)
            self.customer_segments = [
                dict(row) for row in session.execute(query_2).mappings().all()
            ]

            query_3 = text("""
                SELECT 
                    p1.title AS primary_product,
                    p2.title AS secondary_product,
                    COUNT(f1.order_id) AS times_purchased_together
                FROM gold.fact_sales f1
                JOIN gold.fact_sales f2 ON f1.order_id = f2.order_id AND f1.product_id < f2.product_id
                JOIN gold.dim_product p1 ON f1.product_id = p1.product_id
                JOIN gold.dim_product p2 ON f2.product_id = p2.product_id
                GROUP BY p1.title, p2.title
                ORDER BY times_purchased_together DESC LIMIT 10
            """)
            self.product_pairs = [
                dict(row) for row in session.execute(query_3).mappings().all()
            ]

            query_4 = text("""
                WITH activity AS (
                    SELECT customer_id, COUNT(DISTINCT (d.year, d.quarter)) AS q_active
                    FROM gold.fact_sales f
                    JOIN gold.dim_date d ON f.date_key = d.date_key
                    GROUP BY customer_id
                )
                SELECT (COUNT(CASE WHEN q_active > 1 THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0) FROM activity
            """)
            rate_result = session.execute(query_4).scalar()
            self.retention_rate = (
                f"{round(float(rate_result), 1)}%"
                if rate_result is not None
                else "0.0%"
            )

        self.is_loading = False


def section_header(title: str):
    return rx.vstack(
        rx.heading(title, size="6", weight="bold"),
        rx.divider(),
        spacing="2",
        width="100%",
        margin_top="2em",
    )


def index() -> rx.Component:
    return rx.container(
        rx.vstack(
            rx.heading(
                "Gold Layer Insights Report", size="9", weight="bold", padding_y="0.5em"
            ),
            rx.flex(
                rx.card(
                    rx.vstack(
                        rx.text("Retention Rate", size="2", color_scheme="gray"),
                        rx.heading(
                            State.retention_rate, size="8", color_scheme="violet"
                        ),
                        align_items="start",
                        spacing="1",
                    ),
                    width="220px",
                ),
                rx.spacer(),
                rx.button(
                    "Refresh Report",
                    on_click=State.fetch_gold_data,
                    loading=State.is_loading,
                    size="3",
                    variant="soft",
                ),
                width="100%",
                align="end",
            ),
            section_header("Regional Top Performers"),
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell("Region"),
                        rx.table.column_header_cell("Product"),
                        rx.table.column_header_cell("Revenue"),
                    )
                ),
                rx.table.body(
                    rx.foreach(
                        State.top_products,
                        lambda item: rx.table.row(
                            rx.table.cell(item["store_region"]),
                            rx.table.cell(item["product_name"]),
                            rx.table.cell(f"${item['total_revenue']:,}"),
                        ),
                    )
                ),
                variant="surface",
                width="100%",
            ),
            section_header("Customer Segmentation"),
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell("Customer"),
                        rx.table.column_header_cell("Total Spending"),
                        rx.table.column_header_cell("Segment"),
                    )
                ),
                rx.table.body(
                    rx.foreach(
                        State.customer_segments,
                        lambda item: rx.table.row(
                            rx.table.cell(item["customer_full_name"]),
                            rx.table.cell(f"${item['total_spending']:,}"),
                            rx.table.cell(
                                rx.badge(
                                    item["value_segment"],
                                    color_scheme=rx.cond(
                                        item["value_segment"] == "High Value",
                                        "violet",
                                        "gray",
                                    ),
                                )
                            ),
                        ),
                    )
                ),
                variant="surface",
                width="100%",
            ),
            section_header("Product Affinity (Market Basket)"),
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell("Product A"),
                        rx.table.column_header_cell("Product B"),
                        rx.table.column_header_cell("Co-Occurrence"),
                    )
                ),
                rx.table.body(
                    rx.foreach(
                        State.product_pairs,
                        lambda item: rx.table.row(
                            rx.table.cell(item["primary_product"]),
                            rx.table.cell(item["secondary_product"]),
                            rx.table.cell(item["times_purchased_together"]),
                        ),
                    )
                ),
                variant="surface",
                width="100%",
            ),
            spacing="5",
            width="100%",
            padding_bottom="5em",
        ),
        size="4",
        padding_y="2em",
    )


app = rx.App(
    style={"font_family": "Outfit"},
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Outfit:wght@100..900&display=swap"
    ],
    theme=rx.theme(accent_color="violet", radius="large"),
)
app.add_page(index, route="/", on_load=State.fetch_gold_data)
