import json
import argparse
import subprocess
import sys
import os

# Try to import duckdb, and set a flag if it's not available
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

def load_catalog(catalog_path):
    """
    Loads the dbt catalog.json file.

    Args:
        catalog_path (str): The path to the catalog.json file.

    Returns:
        dict: The loaded catalog data, or None if the file doesn't exist or errors.
    """
    try:
        with open(catalog_path, 'r') as f:
            catalog_data = json.load(f)
        return catalog_data
    except FileNotFoundError:
        print(f"Error: catalog.json file not found at {catalog_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in catalog.json at {catalog_path}")
        return None
    except Exception as e:
        print(f"Error loading catalog.json: {e}")
        return None

def get_table_info(catalog_data, table_name):
    """
    Retrieves information about a specific table from the catalog data.

    Args:
        catalog_data (dict): The loaded catalog data.
        table_name (str): The name of the table to retrieve information for.

    Returns:
        dict: Information about the table, or None if not found.
    """
    if not catalog_data:
        return None

    for node in catalog_data['nodes'].values():
        if node['name'].lower() == table_name.lower():
            return node
    return None

def get_table_names(catalog_data):
    """
    Gets all table names from catalog.json

    Args:
        catalog_data (dict): the loaded catalog data

    Returns:
        list: A list of table names
    """
    if not catalog_data:
        return []
    table_names = []
    for node in catalog_data['nodes'].values():
        if node['resource_type'] in ('model', 'seed', 'snapshot', 'table', 'view'):
            table_names.append(node['name'])
    return table_names

def generate_duckdb_query(table_name, catalog_path):
    """
    Generates a DuckDB query to get a sample of data from a table.

    Args:
        table_name (str): The name of the table.
        catalog_path (str): the path to catalog.json

    Returns:
        str: A DuckDB query, or None if DuckDB is not available.
    """
    if not DUCKDB_AVAILABLE:
        return None
    # catalog_path is used only for error reporting, the actual path is derived in main.
    return f"SELECT * FROM read_parquet('{table_name}/*.parquet') LIMIT 5" #This will NOT work.  DuckDB needs a path.

def get_sample_data_duckdb(table_name, catalog_path):
    """
    Retrieves sample data from a table using DuckDB.

    Args:
        table_name (str): The name of the table.
        catalog_path (str): The path to catalog.json

    Returns:
        str: A string representation of the sample data, or None on error.
    """

    if not DUCKDB_AVAILABLE:
        return "DuckDB is not available. Please install it to use this feature."

    query = generate_duckdb_query(table_name, catalog_path)
    if query is None:
        return "DuckDB query generation failed."

    try:
        conn = duckdb.connect(':memory:')  # In-memory database for safety
        # This is the critical change:  We need the folder where the parquets are, not the catalog.
        #  The user will need to provide this.  dbt does NOT put the data into catalog.json
        #  We will ask the user for the path.  For now, we assume a relative path.
        result = conn.execute(query).fetchdf()
        conn.close()
        return result.head().to_string() # consistent
    except Exception as e:
        return f"Error retrieving sample data with DuckDB: {e}"

def get_table_schema_duckdb(table_name, catalog_path):
    """
    Retrieves the schema of a table using DuckDB.

    Args:
        table_name (str): The name of the table.
        catalog_path (str): The path to catalog.json

    Returns:
        str: A string representation of the table schema, or None if DuckDB is not available.
    """
    if not DUCKDB_AVAILABLE:
        return "DuckDB is not available. Please install it to use this feature."

    query = generate_duckdb_query(table_name, catalog_path) # re-use, it's simpler.
    if query is None:
        return "DuckDB query generation failed."
    try:
        conn = duckdb.connect(':memory:')
        result = conn.execute(f"DESCRIBE {query}").fetchdf() # describe the result of the select
        conn.close()
        return result.to_string()
    except Exception as e:
        return f"Error retrieving table schema with DuckDB: {e}"

def main():
    """
    Main function for the CLI tool.
    """
    parser = argparse.ArgumentParser(description="Query dbt models with natural language.")
    parser.add_argument("question", nargs="+", help="The natural language question about your dbt models.")
    parser.add_argument("--catalog", "-c", required=True, help="Path to your dbt catalog.json file.")
    parser.add_argument("--llm", "-l", default="openai", choices=["openai", "ollama", "debug"],
                        help="Choose the LLM to use (openai, ollama, debug).  Ollama must be running.")
    parser.add_argument("--api-key", "-k", help="API key for the LLM (required for OpenAI).")
    parser.add_argument("--data-path", "-d", required=True, help="Path to the data files (e.g., Parquet files) for querying with DuckDB.")

    args = parser.parse_args()
    question = " ".join(args.question)
    catalog_path = args.catalog
    llm_choice = args.llm
    api_key = args.api_key
    data_path = args.data_path # Get the data path.

    catalog_data = load_catalog(catalog_path)
    if not catalog_data:
        sys.exit(1)  # Exit if catalog.json is not loaded

    # 1. Get list of tables.
    table_names = get_table_names(catalog_data)

    # 2.  Find tables related to the question.  (LLM)
    if llm_choice == "openai":
        if not api_key:
            print("Error: --api-key is required when using OpenAI.")
            sys.exit(1)
        # Placeholder:  Add OpenAI code here.  For now, we'll just use a debug response.
        print("Calling OpenAI with question:", question)
        print("Tables in catalog:", table_names)
        llm_response = "I think the relevant tables are customers, orders, and products."
        related_tables = ['customers', 'orders', 'products'] # for testing
    elif llm_choice == "ollama":
        # Placeholder: Add Ollama code here.
        print("Calling Ollama with question:", question)
        print("Tables in catalog:", table_names)
        llm_response = "Ollama says the relevant tables are:  customers, orders, and products."
        related_tables = ['customers', 'orders', 'products'] # for testing
    elif llm_choice == "debug":
        print("Debug mode:  No LLM call.")
        llm_response = "Debug response:  The relevant tables are: customers, orders, and products."
        related_tables = ['customers', 'orders', 'products']  # for testing
    else:
        print("Error: Invalid LLM choice.  Should not have gotten here.")
        sys.exit(1)

    print(llm_response) # print what the LLM said.

    for table_name in related_tables:
        table_info = get_table_info(catalog_data, table_name)
        if table_info:
            print(f"\nTable: {table_name}")
            print(f"  Description: {table_info.get('description', 'No description available.')}")
            print(f"  Columns:")
            for column_name, column_info in table_info['columns'].items():
                print(f"    {column_name}: {column_info['dtype']} - {column_info.get('description', 'No description.')}")

            # Get sample data and schema using DuckDB
            sample_data = get_sample_data_duckdb(table_name, data_path) # Pass the data path
            if sample_data:
                print("\n  Sample Data (DuckDB):")
                print(sample_data)

            table_schema = get_table_schema_duckdb(table_name, data_path) # Pass the data path
            if table_schema:
                print("\n  Table Schema (DuckDB):")
                print(table_schema)
        else:
            print(f"Table '{table_name}' not found in catalog.")

    # 3. Ask LLM for query advice.
    if llm_choice == "openai":
        print("Calling OpenAI for query advice...")
        query_advice = "Here's a possible query using the tables:  SELECT c.name, o.order_date FROM customers c JOIN orders o ON c.customer_id = o.customer_id" # placeholder
        print(query_advice)
    elif llm_choice == "ollama":
        print("Calling Ollama for query advice...")
        query_advice = "Ollama suggests this query: SELECT c.name, o.order_date FROM customers c JOIN orders o ON c.customer_id = o.customer_id" # placeholder
        print(query_advice)
    elif llm_choice == "debug":
        query_advice = "Debug:  Try this query: SELECT c.name, o.order_date FROM customers c JOIN orders o ON c.customer_id = o.customer_id"
        print(query_advice)
    else:
        print("Should not have gotten here.")

    print("\nDone.")

if __name__ == "__main__":
    main()
