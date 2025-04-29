import pandas as pd
import requests
import re
from tqdm import tqdm
from pathlib import Path
import time

from src.utils import extract_cnpjs_from_excel

def fetch_data(cnpj_list: list[str]) -> pd.DataFrame:
    """Fetch company data from ReceitaWS API for a list of CNPJs.

    Args:
        cnpj_list (list[str]): List of CNPJs to query

    Raises:
        ValueError: If CNPJ is invalid or not 14 digits long
        Exception: If API request fails

    Returns:
        pd.DataFrame: DataFrame with company data
    """

    rows = []
    
    for cnpj in tqdm(cnpj_list, desc='Fetching CNPJ data', unit='CNPJ'):
        clean_cnpj = re.sub(r'\D', '', cnpj)
        # if len(clean_cnpj) != 14:
        #     raise ValueError("CNPJ must be 14 digits long")
        
        url = f"https://www.receitaws.com.br/v1/cnpj/{clean_cnpj}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            if data['status'] == 'ERROR':
                time.sleep(37)
                continue
                # raise ValueError("Invalid CNPJ")
            
            row = {
                'cnpj': data['cnpj'],
                'nome': data['nome'],
                'fantasia': data['fantasia'],
                'situacao': data['situacao'],
                'tipo': data['tipo'],
                'porte': data['porte'],
                'natureza_juridica': data['natureza_juridica'],
                'atividade_principal': data['atividade_principal'][0]['text'],
                'logradouro': data['logradouro'],
                'numero': data['numero'],
                'complemento': data['complemento'],
                'municipio': data['municipio'],
                'bairro': data['bairro'],
                'uf': data['uf'],
                'cep': data['cep'],
                'email': data['email'],
                'telefone': data['telefone'],
                'capital_social': data['capital_social'],
                'optante_simples': data['simples']['optante'],
                'optante_simei': data['simei']['optante']
            }
            
            rows.append(row)
            time.sleep(37)
        else:
            continue
            # raise Exception(f"Failed to fetch data for CNPJ {cnpj}: {response.status_code}")

    return pd.DataFrame(rows)
    
if __name__ == "__main__":
    
    file_path = Path.cwd() / 'data' / 'cnpj.xlsx'
    output_path = Path.cwd() / 'data' / 'cnpj_out.xlsx'
    
    cnpj_list = extract_cnpjs_from_excel(file_path)
    
    try:
        df = fetch_data(cnpj_list=cnpj_list)
        df.to_excel(output_path, index=False)
        print(df)
    except Exception as e:
        print(e)
