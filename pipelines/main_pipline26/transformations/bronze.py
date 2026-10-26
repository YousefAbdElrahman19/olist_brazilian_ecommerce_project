import sys

SRC = "/Workspace/Users/yousef.abdelrahman.09@gmail.com/olist_brazilian_ecommerce_project/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from utils.broonze_utils import create_bronze_table

BASE = "/Volumes/olist_brazilian_ecommerce/source/source/archive/"

SOURCES = {
    "customers":                 "olist_customers_dataset.csv",
    "geolocation":               "olist_geolocation_dataset.csv",
    "order_items":               "olist_order_items_dataset.csv",
    "order_payments":            "olist_order_payments_dataset.csv",
    "order_reviews":             "olist_order_reviews_dataset.csv",
    "orders":                    "olist_orders_dataset.csv",
    "products":                  "olist_products_dataset.csv",
    "sellers":                   "olist_sellers_dataset.csv",
    "category_name_translation": "product_category_name_translation.csv",
}

for name, file_name in SOURCES.items():
    create_bronze_table(name, BASE, "csv", pattern=file_name)