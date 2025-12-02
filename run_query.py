import requests
import pandas as pd
import re
import urllib.parse
import json
import time # 引入 time 模块用于暂停

# Wikidata Endpoints
SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'
API_ENDPOINT = 'https://www.wikidata.org/w/api.php'
OUTPUT_FILE = 'minerals_data.csv'

def get_labels_from_api(qids, lang='zh-hans'):
    """Fetches labels for a list of QIDs using the Wikidata API."""
    qids = [q.split('/')[-1] for q in qids if q and q.startswith('http')]
    if not qids:
        return {}

    labels = {}
    for i in range(0, len(qids), 50):
        chunk = qids[i:i + 50]
        params = {
            'action': 'wbgetentities',
            'ids': '|'.join(chunk),
            'props': 'labels',
            'languages': lang,
            'format': 'json'
        }
        try:
            response = requests.get(API_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()
            
            for qid, entity in data.get('entities', {}).items():
                label = entity.get('labels', {}).get(lang, {}).get('value', qid)
                labels[f"http://www.wikidata.org/entity/{qid}"] = label
        except Exception as e:
            print(f"Error fetching API labels for chunk: {e}")
            pass
            
        # 🌟 速率限制：在每次批量 API 调用后暂停 1 秒
        time.sleep(1)
        
    return labels

def execute_sparql_query(sparql_query, is_stage1=False):
    """Executes a single SPARQL query using POST request for stability."""
    
    headers = {
        'Accept': 'application/sparql-results+json',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 遵守 Wikidata 的 User-Agent 要求
        'User-Agent': 'GitHubActions-MineralsBot/1.0 (https://github.com/DavidShiang/wikidata-minerals)'
    }
    
    data = {'query': sparql_query}

    print(f"Executing SPARQL query (Stage {'1' if is_stage1 else '2'})...")
    
    response = requests.post(SPARQL_ENDPOINT, headers=headers, data=data)
    
    if response.status_code != 200:
        print(f"Error executing query: HTTP {response.status_code}")
        print(response.text)
        raise Exception(f"SPARQL query failed with status {response.status_code}")

    try:
        data = response.json()
        results = []
        vars = data['head']['vars']
        for binding in data['results']['bindings']:
            row = {}
            for var in vars:
                row[var] = binding.get(var, {}).get('value', None)
            results.append(row)
        return pd.DataFrame(results)

    except Exception as e:
        print(f"Error processing JSON results: {e}")
        try:
            print("Response content (non-JSON):", response.text[:500])
        except:
            pass
        raise

def process_and_save_data(df):
    # ... (该函数与之前版本保持一致)
    if df.empty:
        print("DataFrame is empty, nothing to process.")
        return

    label_cols = ['item', 'color', 'crystalSystem', 'mainLocation']
    all_qids = set()
    for col in label_cols:
        if col in df.columns:
            all_qids.update(df[col].dropna().unique())

    print(f"Fetching labels for {len(all_qids)} unique QIDs...")
    labels_map = get_labels_from_api(list(all_qids), lang='zh-hans')

    df = df.rename(columns={'item': 'itemURI'})
    df['itemLabel'] = df['itemURI'].map(labels_map).fillna(df['itemURI'])

    for old_col in ['color', 'crystalSystem', 'mainLocation']:
        if old_col in df.columns:
            new_col = f'{old_col}Label'
            df[new_col] = df[old_col].map(labels_map).fillna(df[old_col])
            df = df.drop(columns=[old_col])

    if 'densityNode' in df.columns:
        df['densityValue'] = df['densityNode'].apply(
            lambda x: re.search(r'([0-9\.]+)', str(x)).group(1) if re.search(r'([0-9\.]+)', str(x)) else None
        )
        df = df.drop(columns=['densityNode'])

    final_cols = ['itemLabel', 'chemicalFormula', 'mohsHardness', 'densityValue', 'colorLabel', 'crystalSystemLabel', 'refractiveIndex', 'mainLocationLabel', 'image', 'itemURI']
    final_cols = [col for col in final_cols if col in df.columns]
    
    df = df[final_cols]

    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Successfully retrieved {len(df)} rows.")
    print(f"Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    try:
        # Stage 1: Get Item IDs
        with open('query_stage1.sparql', 'r', encoding='utf-8') as f:
            sparql_stage1 = f.read()
        
        df_ids = execute_sparql_query(sparql_stage1, is_stage1=True)
        item_uris = df_ids['item'].unique().tolist()
        
        if not item_uris:
            print("Stage 1 failed to return any item IDs. Exiting.")
        else:
            print(f"Stage 1 successful. Retrieved {len(item_uris)} item IDs.")
            
            # Stage 2: 批量查询属性
            all_results_df = pd.DataFrame()
            BATCH_SIZE = 50 

            with open('query_stage2.sparql', 'r', encoding='utf-8') as f:
                sparql_stage2_template = f.read()

            num_batches = (len(item_uris) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for i in range(0, len(item_uris), BATCH_SIZE):
                batch_number = i // BATCH_SIZE + 1
                batch_uris = item_uris[i:i + BATCH_SIZE]
                
                values_list = ' '.join(f"<{uri}>" for uri in batch_uris)
                sparql_stage2 = sparql_stage2_template.replace('VALUES ?item { }', f'VALUES ?item {{ {values_list} }}')
                
                print(f"Executing SPARQL query (Stage 2 - Batch {batch_number} of {num_batches})...")
                batch_df = execute_sparql_query(sparql_stage2, is_stage1=False)
                
                all_results_df = pd.concat([all_results_df, batch_df], ignore_index=True)
                
                # 🌟 速率限制：在每次 SPARQL 批次查询后暂停 1 秒
                if batch_number < num_batches:
                    print("Pausing for 1 second to respect rate limits...")
                    time.sleep(1)

            print(f"Stage 2 completed. Total rows retrieved: {len(all_results_df)}")
            
            # Final Step: Process data and save CSV
            process_and_save_data(all_results_df)
            
    except Exception as e:
        print(f"An error occurred during the process: {e}")
