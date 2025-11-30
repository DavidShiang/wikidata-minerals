# run_query.py
import requests
import pandas as pd
import os

# Wikidata SPARQL API endpoint
SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'
QUERY_FILE = 'query.sparql'
OUTPUT_FILE = 'minerals_data.csv'

def run_sparql_query():
    """Reads SPARQL query, executes it against Wikidata, and returns results as a DataFrame."""
    
    # 1. Read the query from the file
    try:
        with open(QUERY_FILE, 'r', encoding='utf-8') as f:
            sparql_query = f.read()
    except FileNotFoundError:
        print(f"Error: {QUERY_FILE} not found.")
        return None

    # 2. Configure the HTTP request to the SPARQL endpoint
    headers = {
        'Accept': 'application/sparql-results+json', # Request JSON format results
        'User-Agent': 'GitHubActions-MineralsBot/1.0 (https://github.com/YOUR_USERNAME/YOUR_REPO_NAME)' 
    }
    params = {'query': sparql_query}

    # 3. Execute the request
    print("Executing SPARQL query...")
    response = requests.get(SPARQL_ENDPOINT, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"Error executing query: HTTP {response.status_code}")
        print(response.text)
        return None

    # 4. Process the JSON results
    try:
        data = response.json()
        
        # Extract column names (variables)
        vars = data['head']['vars']
        
        # Extract rows (bindings)
        results = []
        for binding in data['results']['bindings']:
            row = {}
            for var in vars:
                # Use .get() to safely access value and handle missing fields
                row[var] = binding.get(var, {}).get('value', None)
            results.append(row)
        
        # Convert to Pandas DataFrame
        df = pd.DataFrame(results)
        return df

    except Exception as e:
        print(f"Error processing JSON results: {e}")
        return None

if __name__ == "__main__":
    df_results = run_sparql_query()
    
    if df_results is not None and not df_results.empty:
        # Save the results to a CSV file
        df_results.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"Successfully retrieved {len(df_results)} rows.")
        print(f"Results saved to {OUTPUT_FILE}")
    else:
        print("No results returned or query failed.")
