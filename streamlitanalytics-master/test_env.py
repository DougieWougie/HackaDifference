#!/usr/bin/env python3
"""
Test script to verify .env file is loaded correctly
"""

import os
from dotenv import load_dotenv

def test_env_loading():
    """Test if .env file loads successfully"""
    
    print("Testing .env file loading...")
    print("-" * 50)
    
    # Load .env file
    load_dotenv()
    
    # Check for expected environment variables
    env_vars = {
        "DATABRICKS_SERVER_HOSTNAME": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH"),
        "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN"),
        "DATABRICKS_CATALOG": os.getenv("DATABRICKS_CATALOG", "main"),
        "DATABRICKS_SCHEMA": os.getenv("DATABRICKS_SCHEMA", "academy_data"),
    }
    
    # Display results
    all_loaded = True
    for key, value in env_vars.items():
        if value:
            # Mask sensitive token
            if "TOKEN" in key and value:
                masked_value = value[:8] + "..." + value[-4:] if len(value) > 12 else "***"
                print(f"✅ {key}: {masked_value}")
            else:
                print(f"✅ {key}: {value}")
        else:
            print(f"❌ {key}: Not set")
            if key not in ["DATABRICKS_CATALOG", "DATABRICKS_SCHEMA"]:
                all_loaded = False
    
    print("-" * 50)
    
    if all_loaded:
        print("\n✅ SUCCESS: All required variables loaded from .env")
    else:
        print("\n⚠️  WARNING: Some required variables are missing")
        print("\nMake sure you:")
        print("1. Created a .env file in the same directory as this script")
        print("2. Copied values from .env.example")
        print("3. Filled in your actual Databricks credentials")
    
    print("\n.env file location should be:")
    print(f"   {os.path.join(os.getcwd(), '.env')}")
    
    return all_loaded

if __name__ == "__main__":
    test_env_loading()
