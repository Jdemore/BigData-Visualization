"""Synthesize the sales.csv fixture used by the default --data arg.

50K rows of transactional retail data across 5 regions and 5 product
categories, spanning 2 years. Seeded so regeneration is deterministic:
identical seed produces identical output, which matters for test stability."""

import csv
import os
import random
from datetime import date, timedelta

REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Food"]
PRODUCTS = {
    "Electronics": ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"],
    "Clothing": ["T-Shirt", "Jacket", "Jeans", "Sneakers", "Hat"],
    "Home": ["Lamp", "Pillow", "Rug", "Blender", "Toaster"],
    "Sports": ["Basketball", "Yoga Mat", "Dumbbells", "Bike", "Helmet"],
    "Food": ["Coffee", "Granola", "Pasta", "Olive Oil", "Chocolate"],
}


def generate(output_path: str, n_rows: int = 50_000, seed: int = 42) -> str:
    """Write a seeded sales CSV to output_path and return it."""
    random.seed(seed)
    start_date = date(2023, 1, 1)
    date_range = 730  # two-year span, in days

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "date", "region", "product_category",
            "product_name", "quantity", "unit_price", "revenue", "customer_id",
        ])
        for i in range(1, n_rows + 1):
            d = start_date + timedelta(days=random.randint(0, date_range))
            region = random.choice(REGIONS)
            category = random.choice(CATEGORIES)
            product = random.choice(PRODUCTS[category])
            quantity = random.randint(1, 20)
            unit_price = round(random.uniform(5.0, 999.99), 2)
            revenue = round(quantity * unit_price, 2)
            customer_id = f"C{random.randint(100, 999)}"
            writer.writerow([
                i, d.isoformat(), region, category,
                product, quantity, unit_price, revenue, customer_id,
            ])

    return output_path


if __name__ == "__main__":
    path = generate(os.path.join(os.path.dirname(__file__), "sales.csv"))
    print(f"Generated {path}")
