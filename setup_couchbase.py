import time
import os
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
# We will catch generic exceptions and check the message, as specific
# exception classes for "already exists" errors vary between SDK versions.
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec

# --- Configuration ---
load_dotenv()

CB_HOST = os.getenv("CB_HOST", "localhost")
CB_USERNAME = os.getenv("CB_USERNAME", "Administrator")
CB_PASSWORD = os.getenv("CB_PASSWORD", "password")
CB_USE_TLS = os.getenv("CB_USE_TLS", "false").lower() == "true"

BUCKET_NAME = "sales_poc"
SCOPE_NAME = "_default"
CUSTOMER_COLLECTION_NAME = "customers"
SALES_COLLECTION_NAME = "sales"

# --- Main Setup Logic ---
def setup_database():
    """
    Connects to Couchbase and sets up the required bucket, collections, and indexes.
    """
    print("--- Starting Couchbase Setup ---")

    # Step 1: Connect to the cluster
    try:
        auth = PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)
        
        connection_string = f"couchbases://{CB_HOST}" if CB_USE_TLS else f"couchbase://{CB_HOST}"
        cluster = Cluster(connection_string, ClusterOptions(auth))

        timeout_seconds = int(os.getenv("CB_CONNECT_TIMEOUT", 30))
        cluster.wait_until_ready(timeout=timedelta(seconds=timeout_seconds))
        print("Successfully connected to Couchbase cluster.")
    except Exception as e:
        print(f"Error connecting to Couchbase cluster: {e}")
        return

    # Step 2: Create the bucket
    # try:
    #     bucket_manager = cluster.buckets()
    #     bucket_settings = CreateBucketSettings(
    #         name=BUCKET_NAME,
    #         bucket_type=BucketType.COUCHBASE,
    #         ram_quota_mb=128
    #     )
    #     bucket_manager.create_bucket(bucket_settings)
    #     print(f"Bucket '{BUCKET_NAME}' created successfully.")
    #     time.sleep(5)
    # except Exception as e:
    #     if "bucket already exists" in str(e).lower():
    #         print(f"Bucket '{BUCKET_NAME}' already exists. Skipping creation.")
    #     else:
    #         print(f"An error occurred while creating the bucket: {e}")
    #         return

    # Step 3: Get the bucket and collection manager
    try:
        bucket = cluster.bucket(BUCKET_NAME)
        collection_manager = bucket.collections()
        print(f"Accessed bucket '{BUCKET_NAME}'.")
    except Exception as e:
        print(f"Could not access bucket '{BUCKET_NAME}': {e}")
        return

    # Step 4: Create collections
    _create_collection(collection_manager, CUSTOMER_COLLECTION_NAME, SCOPE_NAME)
    _create_collection(collection_manager, SALES_COLLECTION_NAME, SCOPE_NAME)

    # Step 5: Create indexes
    query_service = cluster.query_indexes()
    _create_index(
        query_service,
        "idx_sales_customer_id",
        SALES_COLLECTION_NAME,
        ["customer_id"]
    )
    _create_index(
        query_service,
        "idx_customers_city_name",
        CUSTOMER_COLLECTION_NAME,
        ["city", "name"]
    )

    # Step 6: Insert sample data
    customer_collection = bucket.scope(SCOPE_NAME).collection(CUSTOMER_COLLECTION_NAME)
    sales_collection = bucket.scope(SCOPE_NAME).collection(SALES_COLLECTION_NAME)
    _insert_sample_data(customer_collection, sales_collection)

    print("\n--- Couchbase Setup Complete ---")

def _create_collection(manager, name, scope):
    """Helper function to create a collection."""
    try:
        spec = CollectionSpec(name, scope_name=scope)
        manager.create_collection(spec)
        print(f"Collection '{scope}.{name}' created.")
        time.sleep(2) # Wait for collection to be ready
    except Exception as e:
        if "collection already exists" in str(e).lower():
            print(f"Collection '{scope}.{name}' already exists. Skipping.")
        else:
            print(f"Error creating collection '{scope}.{name}': {e}")


def _create_index(query_service, index_name, collection_name, fields):
    """Helper function to create a N1QL index."""
    try:
        field_str = ", ".join(f"`{field}`" for field in fields)
        query_service.create_index(
            BUCKET_NAME,
            index_name,
            fields=fields,
            collection_name=collection_name,
            scope_name=SCOPE_NAME
        )
        print(f"Index '{index_name}' on '{collection_name}({field_str})' created.")
    except Exception as e:
        error_str = str(e).lower()
        if "index" in error_str and "already exist" in error_str:
            print(f"Index '{index_name}' already exists. Skipping.")
        else:
            print(f"Error creating index '{index_name}': {e}")


def _insert_sample_data(customer_collection, sales_collection):
    """
    Helper function to generate and insert a large volume of sample documents.
    """
    print("\nGenerating and inserting sample data...")

    # --- Data components for randomization ---
    first_names = ["Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan", "Krishna", "Ishaan", "Anika", "Saanvi", "Aadhya", "Myra", "Aarohi", "Ananya", "Diya", "Pari"]
    last_names = ["Sharma", "Verma", "Gupta", "Singh", "Patel", "Kumar", "Das", "Mehta", "Reddy", "Jain"]
    cities = ["Mumbai", "Delhi", "Bengaluru", "Kolkata", "Chennai", "Hyderabad", "Pune", "Ahmedabad"]
    loyalty_levels = ["Gold", "Silver", "Bronze", "Platinum"]
    products = ["Quantum Widget", "Hyper-Sprocket", "Nano-Gear", "Omega Drive", "Pico-Relay", "Zeta Capacitor", "Epsilon Diode"]

    # --- Generate 50 Customers ---
    customer_ids = []
    print("Generating 50 customers...")
    for i in range(50):
        doc_id = f"cust::{i+1:03d}"
        customer_ids.append(doc_id)
        customer = {
            "name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "city": random.choice(cities),
            "loyalty_level": random.choice(loyalty_levels)
        }
        try:
            customer_collection.upsert(doc_id, customer)
        except Exception as e:
            print(f"Error inserting customer {doc_id}: {e}")
    print("Customer generation complete.")

    # --- Generate 200 Sales ---
    print("\nGenerating 200 sales records...")
    start_date = datetime.now() - timedelta(days=365)
    for i in range(200):
        doc_id = f"sale::{i+1:03d}"
        random_days = random.randint(0, 365)
        sale = {
            "product_name": random.choice(products),
            "sale_amount": random.randint(500, 15000),
            "sale_date": (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d"),
            "customer_id": random.choice(customer_ids) # Link to a random, existing customer
        }
        try:
            sales_collection.upsert(doc_id, sale)
        except Exception as e:
            print(f"Error inserting sale {doc_id}: {e}")
    print("Sales generation complete.")


if __name__ == "__main__":
    setup_database()