# Simple NL Query Agent - Pure Python (Databricks)

This project implements a lightweight Natural Language (NL) Query Agent designed to run within Databricks. It leverages Databricks Foundation Models (specifically Llama 3) to convert user questions into SQL queries, execute them against Unity Catalog tables, and interpret the results back into natural language.

The key feature of this implementation is its **"Pure Python"** approach, relying only on standard libraries (like `requests`, `json`) and `pandas`, avoiding heavy external chains or framework dependencies.

## Features

- **Schema Discovery**: Automatically fetches table names and schemas from a specified Unity Catalog source.
- **Natural Language to SQL**: Uses `databricks-meta-llama-3-1-70b-instruct` to generate Spark SQL queries from questions.
- **Safety & Efficiency**: Implements automatic `LIMIT` clauses and error handling for generated SQL.
- **Result Interpretation**: Summarizes query results back into a concise natural language answer.
- **MLflow Integration**: Wraps the agent as an MLflow Python Model (with the agent class embedded) for portability.
- **Model Serving**: Automates the deployment of the agent as a Databricks Serving Endpoint.

## Prerequisites

- Databricks Workspace with Unity Catalog enabled.
- Access to Databricks Foundation Model APIs (specifically `databricks-meta-llama-3-1-70b-instruct`).
- Permissions to create/read from the specified catalogs and schemas.

## Configuration

Open `databricks_agent_pure_python_v3.py` (or the imported notebook) and configure the top section:

```python
SOURCE_CATALOG = "postgres_emea"       # Catalog containing your data
SOURCE_SCHEMA = "base_data"            # Schema containing your data
TARGET_CATALOG = "hackathon_teams"     # Catalog to store the MLflow model
TARGET_SCHEMA = "hack_to_the_future"   # Schema to store the MLflow model

# Model Settings
FOUNDATION_MODEL = "databricks-meta-llama-3-1-70b-instruct"
MODEL_NAME = "nl_query_agent"
ENDPOINT_NAME = "nl-query-agent-endpoint"
```

## Workflow

The script is structured as a Databricks Notebook and follows this flow:

1.  **Discovery**: Lists tables in the source schema.
2.  **Connection Test**: Verifies access to the Foundation Model endpoint.
3.  **Agent Definition**: Defines the `NLQueryAgent` class which handles:
    - Building schema context (prompt engineering).
    - Generating SQL.
    - Executing SQL via `spark.sql()`.
    - Generating the final answer.
4.  **Testing**: Runs sample queries within the notebook.
5.  **MLflow Registration**:
    - Defines an `MLflowNLAgent` wrapper.
    - Embeds the agent code directly into the model artifact (ensuring the served model is self-contained).
    - Registers the model to Unity Catalog.
6.  **Deployment**: Creates or updates a Databricks Serving Endpoint for real-time inference.

## Usage

### In Notebook
You can use the agent interactively:

```python
agent = NLQueryAgent(...)
result = agent.query("How many rows are in the academy table?")
print(result['answer'])
```

### Via API (Serving Endpoint)
Once deployed, the endpoint accepts REST requests:

```python
import requests

url = f"https://{workspace_url}/serving-endpoints/{ENDPOINT_NAME}/invocations"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

payload = {
    "dataframe_records": [
        {"question": "What are the top 5 performing regions?"}
    ]
}

response = requests.post(url, headers=headers, json=payload)
print(response.json())
```
