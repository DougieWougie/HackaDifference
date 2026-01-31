# Databricks notebook source
# MAGIC %md
# MAGIC # Simple NL Query Agent - Pure Python (No External Dependencies)
# MAGIC 
# MAGIC This version uses only standard Python libraries

# COMMAND ----------

# Configuration
SOURCE_CATALOG = "postgres_emea"
SOURCE_SCHEMA = "base_data"
TARGET_CATALOG = "hackathon_teams"
TARGET_SCHEMA = "hack_to_the_future"

# Model configuration
FOUNDATION_MODEL = "databricks-meta-llama-3-1-70b-instruct"
MODEL_NAME = "nl_query_agent"
ENDPOINT_NAME = "nl-query-agent-endpoint"

print("✓ Configuration set!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Tables

# COMMAND ----------

# Get list of tables
source_tables_df = spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
SOURCE_TABLES = [row.tableName for row in source_tables_df.collect()]

print(f"Found {len(SOURCE_TABLES)} tables:")
for idx, table in enumerate(SOURCE_TABLES[:15], 1):
    print(f"  {idx}. {table}")

if len(SOURCE_TABLES) > 15:
    print(f"  ... and {len(SOURCE_TABLES) - 15} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Foundation Model Access

# COMMAND ----------

import requests
import json

# Get credentials
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

print(f"Workspace: {workspace_url}")
print(f"Model: {FOUNDATION_MODEL}")

# Test the model endpoint
url = f"https://{workspace_url}/serving-endpoints/{FOUNDATION_MODEL}/invocations"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

test_payload = {
    "messages": [
        {
            "role": "user",
            "content": "Say exactly: 'Hello, I am working!'"
        }
    ],
    "max_tokens": 50,
    "temperature": 0
}

print("\nTesting model endpoint...")
response = requests.post(url, headers=headers, json=test_payload, timeout=30)

print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    result = response.json()
    print("✓ Model is accessible!")
    print(f"Response format: {list(result.keys())}")
    
    # Parse response
    if 'choices' in result:
        print(f"Model says: {result['choices'][0]['message']['content']}")
    else:
        print(f"Full response: {json.dumps(result, indent=2)}")
else:
    print(f"✗ Error: {response.status_code}")
    print(response.text[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the Agent

# COMMAND ----------

import pandas as pd
import re
from datetime import datetime

class NLQueryAgent:
    """Natural Language Query Agent using only requests"""
    
    def __init__(self, model_endpoint, catalog, schema, tables):
        self.model_endpoint = model_endpoint
        self.catalog = catalog
        self.schema = schema
        self.tables = tables
        self.full_schema = f"{catalog}.{schema}"
        
        # Get credentials
        self.workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.api_url = f"https://{self.workspace_url}/serving-endpoints/{self.model_endpoint}/invocations"
        
        # Build schema context
        self.schema_context = self._build_schema_context()
        print(f"✓ Agent initialized with {len(tables)} tables")
    
    def _build_schema_context(self):
        """Get schema information for all tables"""
        schemas = []
        
        # Limit to first 15 tables to avoid huge context
        for table in self.tables[:15]:
            full_table = f"{self.full_schema}.{table}"
            try:
                # Get column info
                desc = spark.sql(f"DESCRIBE {full_table}").collect()
                cols = [
                    f"{row.col_name}:{row.data_type}" 
                    for row in desc 
                    if row.col_name and not row.col_name.startswith("#")
                ]
                
                # Limit columns shown
                col_str = ", ".join(cols[:12])
                if len(cols) > 12:
                    col_str += f" ... ({len(cols)} total)"
                
                schemas.append(f"{table}({col_str})")
                
            except Exception as e:
                schemas.append(f"{table}(error: {str(e)[:50]})")
        
        return "\n".join(schemas)
    
    def _call_model(self, prompt, max_tokens=1000):
        """Call the foundation model"""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract content from response
                if 'choices' in result:
                    return result['choices'][0]['message']['content']
                elif 'predictions' in result:
                    return str(result['predictions'][0])
                else:
                    return str(result)
            else:
                return f"API Error {response.status_code}: {response.text[:200]}"
                
        except Exception as e:
            return f"Error calling model: {str(e)}"
    
    def generate_sql(self, question):
        """Generate SQL from natural language question"""
        
        prompt = f"""You are a SQL expert. Generate a SQL query to answer this question.

Database Schema: {self.full_schema}

Available Tables:
{self.schema_context}

Important Rules:
1. Use full table names like {self.full_schema}.table_name
2. Return ONLY the SQL query, no explanation or markdown
3. Use Spark SQL syntax
4. Add LIMIT 100 to prevent huge results
5. Keep it simple and efficient

Question: {question}

SQL Query:"""
        
        response = self._call_model(prompt, max_tokens=500)
        
        # Extract SQL from various formats
        # Try markdown code block
        match = re.search(r'```sql\s*\n(.*?)\n```', response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Try plain code block
        match = re.search(r'```\s*\n(.*?)\n```', response, re.DOTALL)
        if match:
            sql = match.group(1).strip()
            if sql.upper().startswith('SELECT'):
                return sql
        
        # Try to find SELECT statement
        match = re.search(r'(SELECT\s+.*?)(?:;|\n\n|$)', response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip().rstrip(';')
        
        # Return as-is if nothing else works
        return response.strip()
    
    def execute_sql(self, sql):
        """Execute SQL query"""
        try:
            # Add LIMIT if not present
            if 'LIMIT' not in sql.upper():
                sql = sql.rstrip(';') + ' LIMIT 100'
            
            result_df = spark.sql(sql)
            return result_df.toPandas()
            
        except Exception as e:
            return pd.DataFrame({
                "error": [str(e)],
                "sql": [sql]
            })
    
    def interpret_results(self, question, sql, results):
        """Generate natural language answer"""
        
        # Check for errors
        if "error" in results.columns:
            return f"Error executing query: {results['error'].iloc[0]}"
        
        # Check for empty results
        if len(results) == 0:
            return "The query returned no results."
        
        # Format results
        result_summary = results.head(15).to_string(index=False, max_rows=15)
        row_count = len(results)
        
        prompt = f"""Answer this question concisely (2-3 sentences max) based on the SQL query results.

Question: {question}

SQL Query: {sql}

Results ({row_count} rows):
{result_summary}

Provide a clear, direct answer to the question:"""
        
        return self._call_model(prompt, max_tokens=300)
    
    def query(self, question):
        """Full query pipeline: question -> SQL -> results -> answer"""
        start_time = datetime.now()
        
        try:
            print(f"\n{'='*70}")
            print(f"Question: {question}")
            print(f"{'-'*70}")
            
            # Step 1: Generate SQL
            sql = self.generate_sql(question)
            print(f"Generated SQL:\n{sql[:300]}{'...' if len(sql) > 300 else ''}")
            
            # Step 2: Execute SQL
            results = self.execute_sql(sql)
            print(f"Results: {len(results)} rows")
            
            # Step 3: Interpret results
            answer = self.interpret_results(question, sql, results)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "status": "success",
                "question": question,
                "answer": answer,
                "sql": sql,
                "row_count": len(results),
                "execution_time": execution_time,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "question": question,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Agent

# COMMAND ----------

# Create agent
print("Initializing agent...")
agent = NLQueryAgent(
    model_endpoint=FOUNDATION_MODEL,
    catalog=SOURCE_CATALOG,
    schema=SOURCE_SCHEMA,
    tables=SOURCE_TABLES
)

print("\nSchema Context Preview:")
print(agent.schema_context[:500] + "...")

# COMMAND ----------

# Test queries
test_questions = [
    "How many total rows are in the academy table?",
    "What are all the column names in the assessment table?",
    "Show me 5 sample records from the attendance table"
]

print("Running test queries...\n")

for question in test_questions:
    result = agent.query(question)
    
    print(f"\nAnswer: {result.get('answer', result.get('error', 'No answer'))}")
    print(f"Time: {result.get('execution_time', 0):.2f}s")
    print(f"{'='*70}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Testing

# COMMAND ----------

# Try your own question here!
my_question = "What types of assessments are in the database?"

result = agent.query(my_question)

print(f"\n📊 Question: {my_question}")
print(f"\n💡 Answer: {result['answer']}")
print(f"\n🔍 SQL Used:\n{result['sql']}")
print(f"\n⏱️  Time: {result['execution_time']:.2f}s")

# COMMAND ----------

# Show detailed results
print("\nFull Result Details:")
print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Registration (Once Agent Works)

# COMMAND ----------

# MAGIC %pip install mlflow>=2.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import json
import requests
from datetime import datetime

# Also need to re-import the configuration
SOURCE_CATALOG = "postgres_emea"
SOURCE_SCHEMA = "base_data"
TARGET_CATALOG = "hackathon_teams"
TARGET_SCHEMA = "hack_to_the_future"
FOUNDATION_MODEL = "databricks-meta-llama-3-1-70b-instruct"
MODEL_NAME = "nl_query_agent"
ENDPOINT_NAME = "nl-query-agent-endpoint"

# Get SOURCE_TABLES again
SOURCE_TABLES = [row.tableName for row in spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{SOURCE_SCHEMA}").collect()]

print(f"✓ Re-imported configuration after restart")
print(f"  Tables: {len(SOURCE_TABLES)}")

# Set registry to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Create target schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

print(f"✓ Schema ready: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# Re-define the NLQueryAgent class (needed for MLflow)
import re

class NLQueryAgent:
    """Natural Language Query Agent"""
    
    def __init__(self, model_endpoint, catalog, schema, tables):
        self.model_endpoint = model_endpoint
        self.catalog = catalog
        self.schema = schema
        self.tables = tables
        self.full_schema = f"{catalog}.{schema}"
        
        # Get credentials
        self.workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.api_url = f"https://{self.workspace_url}/serving-endpoints/{self.model_endpoint}/invocations"
        
        # Build schema context
        self.schema_context = self._build_schema_context()
    
    def _build_schema_context(self):
        schemas = []
        for table in self.tables[:15]:
            full_table = f"{self.full_schema}.{table}"
            try:
                desc = spark.sql(f"DESCRIBE {full_table}").collect()
                cols = [f"{row.col_name}:{row.data_type}" for row in desc if row.col_name and not row.col_name.startswith("#")]
                col_str = ", ".join(cols[:12])
                if len(cols) > 12:
                    col_str += f" ... ({len(cols)} total)"
                schemas.append(f"{table}({col_str})")
            except:
                pass
        return "\n".join(schemas)
    
    def _call_model(self, prompt, max_tokens=1000):
        import requests
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens, "temperature": 0.1}
        
        try:
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result:
                    return result['choices'][0]['message']['content']
                return str(result)
            return f"API Error {response.status_code}"
        except Exception as e:
            return f"Error: {str(e)}"
    
    def generate_sql(self, question):
        prompt = f"""Generate SQL query to answer this question.

Database: {self.full_schema}
Tables:
{self.schema_context}

Rules:
1. Use full table names: {self.full_schema}.table_name
2. Return ONLY the SQL query
3. Add LIMIT 100

Question: {question}

SQL:"""
        
        response = self._call_model(prompt, max_tokens=500)
        match = re.search(r'```sql\s*\n(.*?)\n```', response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        match = re.search(r'(SELECT\s+.*?)(?:;|\n\n|$)', response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip().rstrip(';')
        return response.strip()
    
    def execute_sql(self, sql):
        try:
            if 'LIMIT' not in sql.upper():
                sql = sql.rstrip(';') + ' LIMIT 100'
            return spark.sql(sql).toPandas()
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})
    
    def interpret_results(self, question, sql, results):
        if "error" in results.columns:
            return f"Error: {results['error'].iloc[0]}"
        if len(results) == 0:
            return "No results found."
        
        result_summary = results.head(15).to_string(index=False)
        prompt = f"""Answer concisely (2-3 sentences):

Question: {question}
Results:
{result_summary}

Answer:"""
        return self._call_model(prompt, max_tokens=300)
    
    def query(self, question):
        try:
            sql = self.generate_sql(question)
            results = self.execute_sql(sql)
            answer = self.interpret_results(question, sql, results)
            
            return {
                "status": "success",
                "question": question,
                "answer": answer,
                "sql": sql,
                "row_count": len(results),
                "execution_time": 0,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "question": question,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

print("✓ NLQueryAgent class defined")

# COMMAND ----------

# Define MLflow wrapper
class MLflowNLAgent(mlflow.pyfunc.PythonModel):
    """MLflow wrapper for the NL Query Agent"""
    
    def load_context(self, context):
        """Initialize agent when model loads"""
        # Store configuration
        self.model_endpoint = FOUNDATION_MODEL
        self.catalog = SOURCE_CATALOG
        self.schema = SOURCE_SCHEMA
        self.tables = SOURCE_TABLES
    
    def predict(self, context, model_input):
        """Prediction interface for MLflow"""
        import pandas as pd
        import requests
        import re
        from datetime import datetime
        
        # Note: NLQueryAgent class must be available in the serving environment
        # We'll save it as a code artifact with the model
        
        # Parse input
        if isinstance(model_input, pd.DataFrame):
            questions = model_input["question"].tolist()
        elif isinstance(model_input, dict):
            questions = [model_input.get("question", "")]
        elif isinstance(model_input, list):
            questions = model_input
        else:
            questions = [str(model_input)]
        
        # Create agent and process queries
        agent = NLQueryAgent(
            model_endpoint=self.model_endpoint,
            catalog=self.catalog,
            schema=self.schema,
            tables=self.tables
        )
        
        results = [agent.query(q) for q in questions]
        return pd.DataFrame(results)

print("✓ MLflowNLAgent wrapper defined")

# COMMAND ----------

# Define MLflow wrapper with embedded agent code
class MLflowNLAgent(mlflow.pyfunc.PythonModel):
    """MLflow wrapper with embedded NL Query Agent"""
    
    def load_context(self, context):
        """Initialize agent configuration when model loads"""
        self.model_endpoint = FOUNDATION_MODEL
        self.catalog = SOURCE_CATALOG
        self.schema = SOURCE_SCHEMA
        self.tables = SOURCE_TABLES
    
    def predict(self, context, model_input):
        """Prediction interface for MLflow"""
        import pandas as pd
        import requests
        import re
        from datetime import datetime
        
        # Define agent class inline so it's available in serving environment
        class NLQueryAgent:
            def __init__(self, model_endpoint, catalog, schema, tables):
                self.model_endpoint = model_endpoint
                self.catalog = catalog
                self.schema = schema
                self.tables = tables
                self.full_schema = f"{catalog}.{schema}"
                
                # Get credentials (works in both notebook and serving)
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    self.workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
                    
                    try:
                        import IPython
                        dbutils = IPython.get_ipython().user_ns["dbutils"]
                        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
                    except:
                        import os
                        self.token = os.environ.get("DATABRICKS_TOKEN", "")
                except:
                    self.workspace_url = ""
                    self.token = ""
                
                self.api_url = f"https://{self.workspace_url}/serving-endpoints/{self.model_endpoint}/invocations"
                self.schema_context = self._build_schema_context()
            
            def _build_schema_context(self):
                schemas = []
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    
                    for table in self.tables[:15]:
                        full_table = f"{self.full_schema}.{table}"
                        try:
                            desc = spark.sql(f"DESCRIBE {full_table}").collect()
                            cols = [f"{row.col_name}:{row.data_type}" for row in desc if row.col_name and not row.col_name.startswith("#")]
                            col_str = ", ".join(cols[:12])
                            if len(cols) > 12:
                                col_str += f" ... ({len(cols)} total)"
                            schemas.append(f"{table}({col_str})")
                        except:
                            pass
                except:
                    schemas = [f"{table}(...)" for table in self.tables[:15]]
                return "\n".join(schemas)
            
            def _call_model(self, prompt, max_tokens=1000):
                headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
                payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens, "temperature": 0.1}
                
                try:
                    response = requests.post(self.api_url, headers=headers, json=payload, timeout=30)
                    if response.status_code == 200:
                        result = response.json()
                        if 'choices' in result:
                            return result['choices'][0]['message']['content']
                        return str(result)
                    return f"API Error {response.status_code}"
                except Exception as e:
                    return f"Error: {str(e)}"
            
            def generate_sql(self, question):
                prompt = f"""Generate SQL query to answer this question.

Database: {self.full_schema}
Tables:
{self.schema_context}

Rules:
1. Use full table names: {self.full_schema}.table_name
2. Return ONLY the SQL query
3. Add LIMIT 100

Question: {question}

SQL:"""
                
                response = self._call_model(prompt, max_tokens=500)
                match = re.search(r'```sql\s*\n(.*?)\n```', response, re.DOTALL | re.IGNORECASE)
                if match:
                    return match.group(1).strip()
                match = re.search(r'(SELECT\s+.*?)(?:;|\n\n|$)', response, re.DOTALL | re.IGNORECASE)
                if match:
                    return match.group(1).strip().rstrip(';')
                return response.strip()
            
            def execute_sql(self, sql):
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    
                    if 'LIMIT' not in sql.upper():
                        sql = sql.rstrip(';') + ' LIMIT 100'
                    return spark.sql(sql).toPandas()
                except Exception as e:
                    return pd.DataFrame({"error": [str(e)]})
            
            def interpret_results(self, question, sql, results):
                if "error" in results.columns:
                    return f"Error: {results['error'].iloc[0]}"
                if len(results) == 0:
                    return "No results found."
                
                result_summary = results.head(15).to_string(index=False)
                prompt = f"""Answer concisely (2-3 sentences):

Question: {question}
Results:
{result_summary}

Answer:"""
                return self._call_model(prompt, max_tokens=300)
            
            def query(self, question):
                try:
                    sql = self.generate_sql(question)
                    results = self.execute_sql(sql)
                    answer = self.interpret_results(question, sql, results)
                    
                    return {
                        "status": "success",
                        "question": question,
                        "answer": answer,
                        "sql": sql,
                        "row_count": len(results),
                        "execution_time": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                except Exception as e:
                    return {
                        "status": "error",
                        "question": question,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
        
        # Parse input
        if isinstance(model_input, pd.DataFrame):
            questions = model_input["question"].tolist()
        elif isinstance(model_input, dict):
            questions = [model_input.get("question", "")]
        elif isinstance(model_input, list):
            questions = model_input
        else:
            questions = [str(model_input)]
        
        # Create agent and process queries
        agent = NLQueryAgent(
            model_endpoint=self.model_endpoint,
            catalog=self.catalog,
            schema=self.schema,
            tables=self.tables
        )
        
        results = [agent.query(q) for q in questions]
        return pd.DataFrame(results)

print("✓ MLflowNLAgent wrapper defined with embedded agent code")
sample_input = pd.DataFrame({
    "question": ["How many rows are there?", "What columns exist?"]
})

sample_output = pd.DataFrame([
    {
        "status": "success",
        "question": "How many rows?",
        "answer": "There are 1000 rows in the table.",
        "sql": "SELECT COUNT(*) FROM table",
        "row_count": 1,
        "execution_time": 1.5,
        "timestamp": "2024-01-01T00:00:00"
    }
])

# COMMAND ----------

# Create sample data for signature
sample_input = pd.DataFrame({
    "question": ["How many rows are there?", "What columns exist?"]
})

sample_output = pd.DataFrame([
    {
        "status": "success",
        "question": "How many rows?",
        "answer": "There are 1000 rows in the table.",
        "sql": "SELECT COUNT(*) FROM table",
        "row_count": 1,
        "execution_time": 1.5,
        "timestamp": "2024-01-01T00:00:00"
    }
])

# COMMAND ----------

# Log and register the model (no external artifacts needed)
from mlflow.models.signature import infer_signature

signature = infer_signature(sample_input, sample_output)

with mlflow.start_run(run_name="nl_query_agent") as run:
    
    # Log parameters
    mlflow.log_param("foundation_model", FOUNDATION_MODEL)
    mlflow.log_param("source_catalog", SOURCE_CATALOG)
    mlflow.log_param("source_schema", SOURCE_SCHEMA)
    mlflow.log_param("num_tables", len(SOURCE_TABLES))
    mlflow.log_param("created_at", datetime.now().isoformat())
    
    # Log the model (agent code is embedded in the class)
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=MLflowNLAgent(),
        signature=signature,
        input_example=sample_input,
        pip_requirements=[
            "pandas>=1.3.0",
            "requests>=2.25.0"
        ]
    )
    
    print(f"✓ Model logged with run_id: {run.info.run_id}")

# Register to Unity Catalog
full_model_name = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{MODEL_NAME}"

registered_model = mlflow.register_model(
    model_uri=f"runs:/{run.info.run_id}/model",
    name=full_model_name,
    tags={
        "type": "nl_query_agent",
        "source": "databricks_foundation_model",
        "created_date": datetime.now().isoformat()
    }
)

print(f"\n✓ Model registered to Unity Catalog!")
print(f"  Name: {full_model_name}")
print(f"  Version: {registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Model Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

try:
    # Try to update existing endpoint
    print(f"Checking for existing endpoint: {ENDPOINT_NAME}")
    existing = w.serving_endpoints.get(ENDPOINT_NAME)
    
    print("Updating existing endpoint...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[
            ServedEntityInput(
                entity_name=full_model_name,
                entity_version=registered_model.version,
                scale_to_zero_enabled=True,
                workload_size="Small"
            )
        ]
    )
    print(f"✓ Endpoint updated: {ENDPOINT_NAME}")
    
except Exception as e:
    if "does not exist" in str(e).lower() or "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Creating new endpoint...")
        w.serving_endpoints.create(
            name=ENDPOINT_NAME,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=full_model_name,
                        entity_version=registered_model.version,
                        scale_to_zero_enabled=True,
                        workload_size="Small"
                    )
                ]
            )
        )
        print(f"✓ Endpoint created: {ENDPOINT_NAME}")
    else:
        print(f"Error: {e}")
        raise

# Wait for endpoint to be ready
print("\nWaiting for endpoint to be ready (this may take 5-10 minutes)...")
w.serving_endpoints.wait_get_serving_endpoint_not_updating(ENDPOINT_NAME)
print("✓ Endpoint is ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Deployed Endpoint

# COMMAND ----------

import requests
import json
import time

# Get endpoint details
endpoint = w.serving_endpoints.get(ENDPOINT_NAME)
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"State: {endpoint.state.ready}")

# Wait a bit for warmup
print("\nWarming up endpoint...")
time.sleep(30)

# Test query
test_payload = {
    "dataframe_records": [
        {"question": "How many rows are in the academy table?"}
    ]
}

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

url = f"https://{workspace_url}/serving-endpoints/{ENDPOINT_NAME}/invocations"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print(f"\nCalling endpoint: {url}")
response = requests.post(url, headers=headers, json=test_payload, timeout=60)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    result = response.json()
    print("\n✓ Endpoint is working!")
    print(json.dumps(result, indent=2, default=str))
else:
    print(f"Error: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Complete!
# MAGIC 
# MAGIC Your Natural Language Query Agent is now:
# MAGIC - ✅ Built and tested
# MAGIC - ✅ Registered in Unity Catalog
# MAGIC - ✅ Deployed as a serving endpoint
# MAGIC - ✅ Ready for API queries
# MAGIC 
# MAGIC ### API Usage:
# MAGIC 
# MAGIC ```python
# MAGIC import requests
# MAGIC 
# MAGIC url = f"https://{workspace_url}/serving-endpoints/{ENDPOINT_NAME}/invocations"
# MAGIC headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# MAGIC 
# MAGIC payload = {
# MAGIC     "dataframe_records": [
# MAGIC         {"question": "What are the top 10 academies by capacity?"}
# MAGIC     ]
# MAGIC }
# MAGIC 
# MAGIC response = requests.post(url, headers=headers, json=payload)
# MAGIC result = response.json()
# MAGIC print(result["predictions"][0]["answer"])
# MAGIC ```
