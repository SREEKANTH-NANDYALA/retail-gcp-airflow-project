import pandas as pd
from datetime import datetime, timedelta
import random

products = [
    ("PROD501", "Running Shoes", "Footwear", 89.99),
    ("PROD502", "Smart Watch", "Electronics", 149.99),
    ("PROD503", "Backpack", "Bags", 59.99),
    ("PROD504", "T-Shirt", "Apparel", 19.99),
    ("PROD505", "Jeans", "Apparel", 49.99),
]

def create_orders(start_id, count, date):
    rows = []
    for i in range(count):
        order_num = start_id + i
        product = random.choice(products)

        rows.append({
            "order_id": f"ORD{order_num}",
            "customer_id": f"CUST{1000+i}",
            "customer_name": f"Customer {i+1}",
            "email": f"customer{i+1}@gmail.com",
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "quantity": random.randint(1, 4),
            "price": product[3],
            "order_status": random.choice(["Completed", "Processing", "Shipped"]),
            "order_date": date,
            "updated_at": f"{date} {random.randint(9,18)}:{random.randint(10,59)}:00"
        })

    return rows

day1 = create_orders(1001, 50, "2026-06-01")

# Day 2 = 45 old records + 10 new records
day2 = day1[:45] + create_orders(1051, 10, "2026-06-02")

pd.DataFrame(day1).to_csv("data/day1_orders.csv", index=False)
pd.DataFrame(day2).to_csv("data/day2_orders.csv", index=False)

print("CSV files created successfully!")