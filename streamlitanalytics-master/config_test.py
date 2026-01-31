# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Configuration and Test
# MAGIC 
# MAGIC Run this first to set up your configuration properly

# COMMAND ----------

# YOUR CONFIGURATION - Update these values
SOURCE_CATALOG = "postgres_emea"  # Where your data currently lives
SOURCE_SCHEMA = "base_data"       # Schema with your tables
TARGET_CATALOG = "hackathon_teams"
TARGET_SCHEMA = "hack_to_the_future"

# Foundation Model
FOUNDATION_MODEL = "databricks-meta-llama-3-1-70b-instruct"
MODEL_NAME = "nl_query_agent"
ENDPOINT_NAME = "nl-query-agent-endpoint"

print("Configuration:")
print(f"  Source: {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print(f"  Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"  Model: {FOUNDATION_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover Available Tables

# COMMAND ----------

# Get list of all tables in source schema
source_tables_df = spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
source_tables = [row.tableName for row in source_tables_df.collect()]

print(f"Found {len(source_tables)} tables in {SOURCE_CATALOG}.{SOURCE_SCHEMA}:")
for idx, table in enumerate(source_tables, 1):
    print(f"  {idx}. {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Select Tables to Use (Optional - or use all)

# COMMAND ----------

# Option 1: Use ALL tables
SOURCE_TABLES = source_tables

# Option 2: Use specific tables only (uncomment and modify)
# SOURCE_TABLES = ["academy", "assessment", "attendance"]  # Add your tables here

print(f"Using {len(SOURCE_TABLES)} tables for the agent:")
for table in SOURCE_TABLES:
    print(f"  - {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Table Access

# COMMAND ----------

# Test that we can actually read these tables
print("Verifying table access...")
accessible_tables = []

for table in SOURCE_TABLES:
    full_table = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}"
    try:
        # Try to count rows
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0].cnt
        accessible_tables.append(table)
        print(f"  ✓ {table}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ {table}: Error - {str(e)[:100]}")

# Update SOURCE_TABLES to only include accessible tables
SOURCE_TABLES = accessible_tables
print(f"\n✓ {len(SOURCE_TABLES)} tables are accessible and ready to use")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Get Table Schemas (for the agent to understand your data)

# COMMAND ----------

def get_table_info():
    """Get schema information for the agent"""
    schema_info = []
    
    for table in SOURCE_TABLES[:10]:  # Limit to first 10 for testing
        full_table = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}"
        
        try:
            # Get columns
            desc_df = spark.sql(f"DESCRIBE {full_table}")
            columns = [
                f"{row.col_name} ({row.data_type})" 
                for row in desc_df.collect() 
                if row.col_name and not row.col_name.startswith("#")
            ]
            
            # Get sample data
            sample_df = spark.sql(f"SELECT * FROM {full_table} LIMIT 2")
            sample_count = sample_df.count()
            
            table_info = f"Table {table}: " + ", ".join(columns[:10])  # First 10 columns
            if len(columns) > 10:
                table_info += f" ... ({len(columns)} total columns)"
            
            schema_info.append(table_info)
            
        except Exception as e:
            schema_info.append(f"Table {table}: Error accessing schema")
    
    return "\n".join(schema_info)

table_schemas = get_table_info()
print("Table Schemas (sample):")
print(table_schemas)

# COMMAND ----------

# MAGIC %md  
# MAGIC ## Step 5: Create Target Schema

# COMMAND ----------

# Create target catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

print(f"✓ Target schema ready: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Build the Agent

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import pandas as pd
import json
import re
from datetime import datetime

class NaturalLanguageQueryAgent:
    """Agent that queries your data using natural language"""
    
    def __init__(self, 
                 model_endpoint: str,
                 catalog: str,
                 schema: str,
                 tables: list):
        
        self.model_endpoint = model_endpoint
        self.catalog = catalog
        self.schema = schema
        self.tables = tables
        self.full_schema = f"{catalog}.{schema}"
        self.workspace = WorkspaceClient()
        
        # Cache table schemas
        self.table_schemas = self._build_schema_context()
        print(f"✓ Agent initialized with {len(tables)} tables")
    
    def _build_schema_context(self) -> str:
        """Build schema context for the LLM"""
        schemas = []
        for table in self.tables[:20]:  # Limit context size
            full_table = f"{self.full_schema}.{table}"
            try:
                desc_df = spark.sql(f"DESCRIBE {full_table}")
                cols = [
                    f"{r.col_name}:{r.data_type}" 
                    for r in desc_df.collect() 
                    if r.col_name and not r.col_name.startswith("#")
                ]
                schemas.append(f"{table}({', '.join(cols[:15])})")  # First 15 cols
            except:
                pass
        return "\n".join(schemas)
    
    def _call_model(self, prompt: str) -> str:
        """Call the foundation model"""
        try:
            response = self.workspace.serving_endpoints.query(
                name=self.model_endpoint,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1500
            )
            return response.choices[0].message.content if hasattr(response, 'choices') else str(response)
        except Exception as e:
            return f"Model error: {str(e)}"
    
    def _generate_sql(self, question: str) -> str:
        """Generate SQL from natural language"""
        prompt = f"""Generate a SQL query for this question. Return ONLY the SQL query, no explanation.

Database: {self.full_schema}
Tables:
{self.table_schemas}

Question: {question}

SQL Query:"""
        
        response = self._call_model(prompt)
        
        # Extract SQL
        sql_match = re.search(r'```sql\n(.*?)\n```', response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()
        
        # Try to find SELECT statement
        select_match = re.search(r'(SELECT\s+.*?)(?:;|$)', response, re.DOTALL | re.IGNORECASE)
        if select_match:
            return select_match.group(1).strip()
        
        return response.strip()
    
    def _execute_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL query"""
        try:
            return spark.sql(sql).limit(100).toPandas()
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})
    
    def _format_answer(self, question: str, sql: str, results: pd.DataFrame) -> str:
        """Generate natural language answer"""
        if "error" in results.columns:
            return f"Error: {results['error'].iloc[0]}"
        
        results_text = results.head(10).to_string(index=False)
        
        prompt = f"""Provide a concise answer (2-3 sentences) to this question based on the query results.

Question: {question}
Query Results:
{results_text}

Answer:"""
        
        return self._call_model(prompt).strip()
    
    def query(self, question: str) -> dict:
        """Query with natural language"""
        start = datetime.now()
        
        try:
            print(f"Question: {question}")
            
            sql = self._generate_sql(question)
            print(f"SQL: {sql[:150]}...")
            
            results = self._execute_sql(sql)
            answer = self._format_answer(question, sql, results)
            
            duration = (datetime.now() - start).total_seconds()
            
            return {
                "status": "success",
                "question": question,
                "answer": answer,
                "sql": sql,
                "result_count": len(results),
                "execution_time": duration
            }
        except Exception as e:
            return {
                "status": "error",
                "question": question,
                "error": str(e)
            }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test the Agent

# COMMAND ----------

# Initialize agent
print("Creating agent...")
agent = NaturalLanguageQueryAgent(
    model_endpoint=FOUNDATION_MODEL,
    catalog=SOURCE_CATALOG,
    schema=SOURCE_SCHEMA,
    tables=SOURCE_TABLES
)

# COMMAND ----------

# Test with some questions
test_questions = [
    "How many rows are in the academy table?",
    "What are the column names in the assessment table?",
    "Show me 5 sample records from any table"
]

for question in test_questions:
    print("\n" + "="*80)
    result = agent.query(question)
    print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Once the agent works above, continue with:
# MAGIC 1. MLflow model registration (Step 8-9 from the full notebook)
# MAGIC 2. Endpoint deployment (Step 10)
# MAGIC 3. API testing (Step 11)
# MAGIC 
# MAGIC The key fix was using SOURCE_CATALOG and SOURCE_SCHEMA correctly!