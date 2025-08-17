import os
from datetime import timedelta
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException

# --- Configuration ---
CB_HOST = os.getenv("CB_HOST")
CB_USERNAME = os.getenv("CB_USERNAME")
CB_PASSWORD = os.getenv("CB_PASSWORD")
CB_USE_TLS = os.getenv("CB_USE_TLS", "false").lower() == "true"
BUCKET_NAME = "sales_poc"


# --- Database Connection ---
try:
    auth = PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)

    # Determine connection string based on TLS setting
    connection_string = f"couchbases://{CB_HOST}" if CB_USE_TLS else f"couchbase://{CB_HOST}"
    # For Capella, the SDK automatically handles TLS config with the 'couchbases' scheme.

    cluster = Cluster(connection_string, ClusterOptions(auth))
    timeout_seconds = int(os.getenv("CB_CONNECT_TIMEOUT", 30))
    cluster.wait_until_ready(timeout=timedelta(seconds=timeout_seconds))

    bucket = cluster.bucket(BUCKET_NAME)
    print("Database connection successful.")
except CouchbaseException as e:
    print(f"Error connecting to Couchbase: {e}")
    # In a real app, you might want to exit or have a retry mechanism
    cluster = None
    bucket = None

# --- Database Functions ---

def execute_n1ql_query(query: str):
    """
    Executes a N1QL query against the Couchbase cluster.

    Args:
        query: The N1QL query string to execute.

    Returns:
        A list of query results, or raises an exception on failure.
    """
    if not cluster:
        raise ConnectionError("Couchbase cluster is not connected.")
    
    try:
        print(f"Executing N1QL Query: {query}")
        result = cluster.query(query)
        return [row for row in result.rows()]
    except CouchbaseException as e:
        print(f"N1QL query failed: {e}")
        # Re-raise the exception to be handled by the API endpoint
        raise
