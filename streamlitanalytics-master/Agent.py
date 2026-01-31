# Databricks notebook source
# MAGIC %md
# MAGIC # Natural Language Query Agent with Databricks (No LangChain)
# MAGIC 
# MAGIC This notebook creates an agent that can query your data using natural language,
# MAGIC registers it in Unity Catalog, and deploys it as an API endpoint.
# MAGIC 
# MAGIC **Uses only Databricks native tools - no external dependencies needed!**
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Access to Databricks Foundation Models
# MAGIC - Permissions to create schemas and models in Unity Catalog
# MAGIC - Target data tables accessible in your catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries (Minimal)

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Dependencies

# COMMAND ----------

import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
import json
import os
import re
from typing import Dict, List, Any, Optional
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configuration

# COMMAND ----------

# Configuration parameters
CATALOG = "hackathon_teams"
SCHEMA = "hack_to_the_future"
MODEL_NAME = "nl_query_agent"
ENDPOINT_NAME = "nl-query-agent-endpoint"

# Databricks Foundation Model
# Available models: databricks-meta-llama-3-1-405b-instruct, databricks-meta-llama-3-1-70b-instruct
FOUNDATION_MODEL = "databricks-meta-llama-3-1-70b-instruct"

# Your data configuration - UPDATE THESE
SOURCE_CATALOG = "your_catalog"  # e.g., "main"
SOURCE_SCHEMA = "your_schema"    # e.g., "sales_data"
SOURCE_TABLES = ["table1", "table2"]  # List your tables

# Create fully qualified names
FULL_SCHEMA_NAME = f"{CATALOG}.{SCHEMA}"
FULL_MODEL_NAME = f"{FULL_SCHEMA_NAME}.{MODEL_NAME}"

print(f"Configuration:")
print(f"  Model will be registered as: {FULL_MODEL_NAME}")
print(f"  Endpoint name: {ENDPOINT_NAME}")
print(f"  Source data: {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print(f"  Foundation Model: {FOUNDATION_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ensure Schema Exists

# COMMAND ----------

# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")

print(f"✓ Schema {FULL_SCHEMA_NAME} is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Get Table Schema Information

# COMMAND ----------

def get_table_schemas() -> str:
    """Get schema information for all source tables"""
    schema_info = []
    
    for table in SOURCE_TABLES:
        full_table_name = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}"
        
        try:
            # Get table description
            desc_df = spark.sql(f"DESCRIBE TABLE {full_table_name}")
            columns = desc_df.select("col_name", "data_type").collect()
            
            # Format column information
            col_list = []
            for col in columns:
                if col.col_name and not col.col_name.startswith("#"):  # Skip partition info
                    col_list.append(f"  - {col.col_name} ({col.data_type})")
            
            # Get sample data
            sample_df = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 3")
            sample_data = sample_df.toPandas().head(3).to_string(index=False)
            
            # Get row count
            count_df = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}")
            row_count = count_df.collect()[0].count
            
            table_info = f"""
Table: {table}
Full Name: {full_table_name}
Row Count: {row_count:,}
Columns:
{chr(10).join(col_list)}

Sample Data (first 3 rows):
{sample_data}
"""
            schema_info.append(table_info)
            
        except Exception as e:
            schema_info.append(f"Table: {table}\nError: {str(e)}")
    
    return "\n" + "="*80 + "\n".join(schema_info)

# Test it
print("Fetching table schemas...")
schema_text = get_table_schemas()
print(schema_text[:500] + "...")  # Print first 500 chars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Natural Language Query Agent

# COMMAND ----------

class NaturalLanguageQueryAgent:
    """
    An agent that converts natural language queries to SQL and executes them
    against your Databricks tables using Foundation Models.
    """
    
    def __init__(self, 
                 model_endpoint: str = FOUNDATION_MODEL,
                 catalog: str = SOURCE_CATALOG,
                 schema: str = SOURCE_SCHEMA,
                 tables: List[str] = SOURCE_TABLES):
        
        self.model_endpoint = model_endpoint
        self.catalog = catalog
        self.schema = schema
        self.tables = tables
        self.full_schema = f"{catalog}.{schema}"
        
        # Get workspace client for API calls
        self.workspace = WorkspaceClient()
        
        # Cache table schemas
        self.table_schemas = self._get_table_info()
        
        print(f"✓ Agent initialized with model: {model_endpoint}")
    
    def _get_table_info(self) -> str:
        """Get and cache table schema information"""
        schema_parts = []
        
        for table in self.tables:
            full_table = f"{self.full_schema}.{table}"
            try:
                desc_df = spark.sql(f"DESCRIBE {full_table}")
                columns = desc_df.select("col_name", "data_type").collect()
                
                col_info = ", ".join([
                    f"{c.col_name} {c.data_type}" 
                    for c in columns 
                    if c.col_name and not c.col_name.startswith("#")
                ])
                
                schema_parts.append(f"Table {table}: {col_info}")
            except Exception as e:
                schema_parts.append(f"Table {table}: Error - {str(e)}")
        
        return "\n".join(schema_parts)
    
    def _call_foundation_model(self, prompt: str) -> str:
        """Call the Databricks Foundation Model API"""
        try:
            response = self.workspace.serving_endpoints.query(
                name=self.model_endpoint,
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=0.1,
                max_tokens=2000
            )
            
            # Extract the response text
            if hasattr(response, 'choices') and len(response.choices) > 0:
                return response.choices[0].message.content
            else:
                return str(response)
                
        except Exception as e:
            return f"Error calling model: {str(e)}"
    
    def _generate_sql(self, question: str) -> str:
        """Generate SQL query from natural language question"""
        
        prompt = f"""You are a SQL expert for Databricks. Given a natural language question, generate a valid SQL query.

DATABASE SCHEMA:
{self.table_schemas}

RULES:
1. Use the full table names: {self.full_schema}.table_name
2. Return ONLY the SQL query, no explanation
3. Use standard SQL syntax compatible with Spark SQL
4. For date/time operations, use Spark SQL functions
5. Keep queries efficient - add LIMIT clauses when appropriate

QUESTION: {question}

SQL QUERY:"""
        
        response = self._call_foundation_model(prompt)
        
        # Extract SQL from response (handle markdown code blocks)
        sql_pattern = r'```sql\n(.*?)\n```'
        sql_match = re.search(sql_pattern, response, re.DOTALL | re.IGNORECASE)
        
        if sql_match:
            return sql_match.group(1).strip()
        
        # If no markdown, try to find SELECT statement
        sql_pattern2 = r'(SELECT\s+.*?(?:;|$))'
        sql_match2 = re.search(sql_pattern2, response, re.DOTALL | re.IGNORECASE)
        
        if sql_match2:
            return sql_match2.group(1).strip().rstrip(';')
        
        # Return the whole response if we can't extract SQL
        return response.strip()
    
    def _execute_sql(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            result_df = spark.sql(sql_query)
            return result_df.toPandas()
        except Exception as e:
            # Return error as DataFrame
            return pd.DataFrame({
                "error": [str(e)],
                "query": [sql_query]
            })
    
    def _interpret_results(self, question: str, sql_query: str, results_df: pd.DataFrame) -> str:
        """Generate natural language answer from query results"""
        
        # Check for errors
        if "error" in results_df.columns:
            return f"Error executing query: {results_df['error'].iloc[0]}"
        
        # Format results for the model
        results_text = results_df.to_string(index=False, max_rows=20)
        
        prompt = f"""Given the following SQL query and its results, provide a clear, concise answer to the original question.

QUESTION: {question}

SQL QUERY:
{sql_query}

QUERY RESULTS:
{results_text}

Provide a natural language answer that:
1. Directly answers the question
2. Highlights key findings
3. Includes relevant numbers/statistics
4. Is concise (2-3 sentences)

ANSWER:"""
        
        answer = self._call_foundation_model(prompt)
        return answer.strip()
    
    def query(self, question: str, return_sql: bool = False) -> Dict[str, Any]:
        """
        Process a natural language query and return results
        
        Args:
            question: Natural language question about the data
            return_sql: If True, include SQL query in response
            
        Returns:
            Dictionary containing the answer and metadata
        """
        start_time = datetime.now()
        
        try:
            # Step 1: Generate SQL
            print(f"Generating SQL for: {question}")
            sql_query = self._generate_sql(question)
            print(f"Generated SQL: {sql_query[:200]}...")
            
            # Step 2: Execute SQL
            print("Executing query...")
            results_df = self._execute_sql(sql_query)
            
            # Step 3: Interpret results
            print("Interpreting results...")
            answer = self._interpret_results(question, sql_query, results_df)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            response = {
                "status": "success",
                "question": question,
                "answer": answer,
                "result_count": len(results_df),
                "execution_time_seconds": execution_time,
                "model": self.model_endpoint,
                "timestamp": datetime.now().isoformat()
            }
            
            if return_sql:
                response["sql_query"] = sql_query
                response["raw_results"] = results_df.to_dict(orient="records")
            
            return response
            
        except Exception as e:
            return {
                "status": "error",
                "question": question,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def predict(self, model_input):
        """MLflow predict interface"""
        if isinstance(model_input, pd.DataFrame):
            questions = model_input["question"].tolist()
        elif isinstance(model_input, dict):
            questions = [model_input.get("question", "")]
        elif isinstance(model_input, list):
            questions = model_input
        else:
            questions = [str(model_input)]
        
        results = [self.query(q) for q in questions]
        return pd.DataFrame(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test the Agent Locally

# COMMAND ----------

# Initialize and test the agent
print("Initializing agent...")
agent = NaturalLanguageQueryAgent()

# Test with a sample query
test_question = "How many total rows are in each table?"
print(f"\nTest Question: {test_question}")
print("-" * 80)

result = agent.query(test_question, return_sql=True)
print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Define MLflow Model Wrapper

# COMMAND ----------

class DatabricksNLQueryAgent(mlflow.pyfunc.PythonModel):
    """
    MLflow wrapper for the Natural Language Query Agent
    """
    
    def load_context(self, context):
        """Load the agent when the model is loaded"""
        # Import here to ensure it's available during serving
        import pandas as pd
        from datetime import datetime
        import json
        
        self.model_endpoint = FOUNDATION_MODEL
        self.catalog = SOURCE_CATALOG
        self.schema = SOURCE_SCHEMA
        self.tables = SOURCE_TABLES
    
    def predict(self, context, model_input):
        """
        Handle predictions from the model
        
        Input can be:
        - DataFrame with 'question' column
        - Dictionary with 'question' key
        - List of questions
        - Single question string
        """
        # Recreate agent (needed for serving environment)
        agent = NaturalLanguageQueryAgent(
            model_endpoint=self.model_endpoint,
            catalog=self.catalog,
            schema=self.schema,
            tables=self.tables
        )
        
        return agent.predict(model_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Log and Register Model to Unity Catalog

# COMMAND ----------

# Set MLflow registry to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Create sample input/output for signature
sample_input = pd.DataFrame({
    "question": ["What is the total count?", "Show me recent records"]
})

sample_output = pd.DataFrame({
    "status": ["success"],
    "question": ["What is the total count?"],
    "answer": ["The total count is 100"],
    "result_count": [1],
    "execution_time_seconds": [1.5],
    "model": [FOUNDATION_MODEL],
    "timestamp": [datetime.now().isoformat()]
})

# Define model signature
from mlflow.models.signature import infer_signature
signature = infer_signature(sample_input, sample_output)

# Log the model
with mlflow.start_run(run_name="nl_query_agent") as run:
    
    # Log parameters
    mlflow.log_param("foundation_model", FOUNDATION_MODEL)
    mlflow.log_param("source_catalog", SOURCE_CATALOG)
    mlflow.log_param("source_schema", SOURCE_SCHEMA)
    mlflow.log_param("source_tables", ",".join(SOURCE_TABLES))
    
    # Log the model
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=DatabricksNLQueryAgent(),
        signature=signature,
        input_example=sample_input,
        pip_requirements=[
            "databricks-sdk>=0.18.0",
            "mlflow>=2.9.0"
        ]
    )
    
    print(f"✓ Model logged with run_id: {run.info.run_id}")

# Register to Unity Catalog
registered_model = mlflow.register_model(
    model_uri=f"runs:/{run.info.run_id}/model",
    name=FULL_MODEL_NAME,
    tags={
        "type": "nl_query_agent",
        "source": "databricks_foundation_model",
        "created_date": datetime.now().isoformat()
    }
)

print(f"✓ Model registered to Unity Catalog: {FULL_MODEL_NAME}")
print(f"  Version: {registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Model Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput
)

# Initialize workspace client
w = WorkspaceClient()

# Check if endpoint already exists
try:
    existing_endpoint = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"Endpoint {ENDPOINT_NAME} already exists. Updating...")
    
    # Update the endpoint
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[
            ServedEntityInput(
                entity_name=FULL_MODEL_NAME,
                entity_version=registered_model.version,
                scale_to_zero_enabled=True,
                workload_size="Small"
            )
        ]
    )
    print(f"✓ Endpoint updated successfully")
    
except Exception as e:
    if "does not exist" in str(e).lower() or "RESOURCE_DOES_NOT_EXIST" in str(e):
        print(f"Creating new endpoint: {ENDPOINT_NAME}")
        
        # Create new endpoint
        w.serving_endpoints.create(
            name=ENDPOINT_NAME,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=FULL_MODEL_NAME,
                        entity_version=registered_model.version,
                        scale_to_zero_enabled=True,
                        workload_size="Small"
                    )
                ]
            )
        )
        print(f"✓ Endpoint created successfully")
    else:
        print(f"Error: {e}")
        raise e

# Wait for endpoint to be ready
print("Waiting for endpoint to be ready (this may take 5-10 minutes)...")
w.serving_endpoints.wait_get_serving_endpoint_not_updating(ENDPOINT_NAME)
print(f"✓ Endpoint {ENDPOINT_NAME} is ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Test the API Endpoint

# COMMAND ----------

import requests
import time

# Get workspace context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.tags().get("browserHostName").get()
token = ctx.apiToken().get()

# Test the endpoint
def query_endpoint(question: str) -> dict:
    """Query the serving endpoint with a natural language question"""
    
    url = f"https://{workspace_url}/serving-endpoints/{ENDPOINT_NAME}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "dataframe_records": [{"question": question}]
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        return {
            "error": f"Status {response.status_code}",
            "message": response.text
        }

# Wait a bit for endpoint to fully initialize
print("Waiting for endpoint to warm up...")
time.sleep(30)

# Test queries
test_questions = [
    "How many rows are in each table?",
    "What are the column names in the first table?",
    "Show me summary statistics"
]

for question in test_questions:
    print(f"\n{'='*80}")
    print(f"Question: {question}")
    print(f"{'='*80}")
    
    result = query_endpoint(question)
    print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Create Helper Functions

# COMMAND ----------

def ask_data(question: str, return_raw: bool = False) -> Any:
    """
    Convenience function to query your data using natural language
    
    Args:
        question: Natural language question about your data
        return_raw: If True, return full response; if False, return just the answer
        
    Returns:
        Answer to the question or full response dict
    """
    result = query_endpoint(question)
    
    if return_raw:
        return result
    
    # Extract the answer from the response
    if "predictions" in result and len(result["predictions"]) > 0:
        prediction = result["predictions"][0]
        if "answer" in prediction:
            return prediction["answer"]
    
    return result

# Test the helper
print("Testing helper function...")
answer = ask_data("What data do we have available?")
print(f"\nAnswer: {answer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Usage Documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎉 Setup Complete!
# MAGIC 
# MAGIC Your Natural Language Query Agent is now deployed and ready to use.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Quick Start - Using in Databricks
# MAGIC 
# MAGIC ```python
# MAGIC # Simple usage
# MAGIC ask_data("How many rows are in each table?")
# MAGIC ask_data("What are the top 10 records by value?")
# MAGIC ask_data("Show me the distribution of categories")
# MAGIC 
# MAGIC # Get full response with metadata
# MAGIC result = ask_data("What's the average amount?", return_raw=True)
# MAGIC print(f"Query took {result['predictions'][0]['execution_time_seconds']} seconds")
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### API Usage - From Outside Databricks
# MAGIC 
# MAGIC ```python
# MAGIC import requests
# MAGIC 
# MAGIC workspace_url = "your-workspace.cloud.databricks.com"
# MAGIC token = "your-personal-access-token"
# MAGIC 
# MAGIC url = f"https://{workspace_url}/serving-endpoints/nl-query-agent-endpoint/invocations"
# MAGIC headers = {
# MAGIC     "Authorization": f"Bearer {token}",
# MAGIC     "Content-Type": "application/json"
# MAGIC }
# MAGIC 
# MAGIC payload = {
# MAGIC     "dataframe_records": [
# MAGIC         {"question": "What are the top products by revenue this month?"}
# MAGIC     ]
# MAGIC }
# MAGIC 
# MAGIC response = requests.post(url, headers=headers, json=payload)
# MAGIC result = response.json()
# MAGIC 
# MAGIC print(result["predictions"][0]["answer"])
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### What You Can Ask
# MAGIC 
# MAGIC ✅ Aggregations: "What's the total/average/sum of X?"  
# MAGIC ✅ Filtering: "Show me records where X equals Y"  
# MAGIC ✅ Top N: "What are the top 10 items by sales?"  
# MAGIC ✅ Comparisons: "Compare X between regions"  
# MAGIC ✅ Trends: "Show me growth over time"  
# MAGIC ✅ Joins: "Combine data from table A and B"  
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Model Details
# MAGIC 
# MAGIC - **Model**: `{FULL_MODEL_NAME}`
# MAGIC - **Endpoint**: `{ENDPOINT_NAME}`
# MAGIC - **Foundation Model**: `{FOUNDATION_MODEL}`
# MAGIC - **Version**: `{registered_model.version}`
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 
# MAGIC 1. **Test with your queries**: Try different questions about your data
# MAGIC 2. **Monitor performance**: Check endpoint metrics in the Serving UI
# MAGIC 3. **Optimize**: Adjust temperature and prompts for better results
# MAGIC 4. **Scale**: Increase workload size if needed for production
# MAGIC 5. **Integrate**: Add to dashboards, apps, or Slack bots
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Troubleshooting
# MAGIC 
# MAGIC - **Slow responses**: Increase workload size in endpoint config
# MAGIC - **Incorrect SQL**: Adjust the SQL generation prompt
# MAGIC - **Timeout errors**: Add LIMIT clauses or optimize queries
# MAGIC - **Permission errors**: Check Unity Catalog grants

# COMMAND ----------

print("=" * 80)
print("🎉 Natural Language Query Agent Setup Complete!")
print("=" * 80)
print(f"\nModel: {FULL_MODEL_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Status: Ready to use")
print("\n✨ You can now query your data using natural language!")
print("=" * 80)