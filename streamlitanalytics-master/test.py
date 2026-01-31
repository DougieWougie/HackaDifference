# Method 1: Using Databricks SQL Connector (Recommended)
from databricks import sql
import os

def query_with_sql_connector():
    with sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path="/sql/1.0/warehouses/warehouse_id",
        access_token=os.getenv("DATABRICKS_TOKEN")
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM main.default.my_table LIMIT 10")
            return cursor.fetchall_arrow().to_pandas()

# Method 2: Using Databricks SDK
from databricks.sdk import WorkspaceClient

def query_with_sdk():
    w = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    # Execute SQL query
    result = w.statement_execution.execute_statement(
        warehouse_id="warehouse_id",
        statement="SELECT * FROM main.default.my_table LIMIT 10"
    )
    return result

# Method 3: Reading from Unity Catalog Volumes
import pandas as pd

def read_from_volume():
    # Apps can access volumes using standard file operations
    file_path = "/Volumes/main/default/my_volume/data.csv"
    return pd.read_csv(file_path)

# Method 4: Using REST API
import requests

def query_with_rest_api():
    headers = {
        "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "warehouse_id": "warehouse_id",
        "statement": "SELECT * FROM main.default.my_table LIMIT 10",
        "wait_timeout": "30s"
    }
    
    response = requests.post(
        f"{os.getenv('DATABRICKS_HOST')}/api/2.0/sql/statements",
        headers=headers,
        json=payload
    )
    return response.json()s