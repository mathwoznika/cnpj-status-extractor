import pandas as pd
import requests
import re
from tqdm import tqdm
from pathlib import Path
import time
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils import extract_cnpjs_from_excel

def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[408, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def save_checkpoint(output_path, df, checkpoint_path):
    """Salva o progresso atual"""
    df.to_excel(output_path, index=False)
    with open(checkpoint_path, 'w') as f:
        json.dump({'last_cnpj': df['cnpj'].iloc[-1]}, f)

def load_checkpoint(checkpoint_path, cnpj_list):
    """Carrega o ponto de parada anterior"""
    try:
        with open(checkpoint_path, 'r') as f:
            data = json.load(f)
        last_index = cnpj_list.index(data['last_cnpj'])
        return cnpj_list[last_index + 1:]
    except:
        return cnpj_list

def fetch_single_cnpj(session, cnpj):
    """Busca dados de um único CNPJ com tratamento robusto"""
    clean_cnpj = re.sub(r'\D', '', cnpj)
    url = f"https://www.receitaws.com.br/v1/cnpj/{clean_cnpj}"
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == 'ERROR':
            return None
        
        return {
            'cnpj': data['cnpj'],
            'nome': data['nome'],
            'fantasia': data['fantasia'],
            'situacao': data['situacao'],
            'tipo': data['tipo'],
            'porte': data['porte'],
            'natureza_juridica': data['natureza_juridica'],
            'atividade_principal': data['atividade_principal'][0]['text'] if data['atividade_principal'] else '',
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
            'optante_simples': data['simples']['optante'] if 'simples' in data else '',
            'optante_simei': data['simei']['optante'] if 'simei' in data else ''
        }
    except Exception as e:
        print(f"Erro no CNPJ {clean_cnpj}: {str(e)}")
        return None

def fetch_data(cnpj_list: list[str], output_path: Path, checkpoint_interval=10):
    checkpoint_path = output_path.parent / 'checkpoint.json'
    remaining_cnpjs = load_checkpoint(checkpoint_path, cnpj_list)
    
    session = create_session()
    rows = []
    
    for i, cnpj in enumerate(tqdm(remaining_cnpjs, desc='Processando CNPJs')):
        result = fetch_single_cnpj(session, cnpj)
        
        if result:
            rows.append(result)
            
            # Salva checkpoint periodicamente
            if i % checkpoint_interval == 0:
                df = pd.DataFrame(rows)
                save_checkpoint(output_path, df, checkpoint_path)
        
        time.sleep(37)  # Respeita o limite da API
        
    return pd.DataFrame(rows)

if __name__ == "__main__":
    file_path = Path.cwd() / 'data' / 'cnpj.xlsx'
    output_path = Path.cwd() / 'data' / 'cnpj_out.xlsx'
    
    cnpj_list = extract_cnpjs_from_excel(file_path)
    
    try:
        df = fetch_data(cnpj_list, output_path)
        df.to_excel(output_path, index=False)
        print("Processamento concluído com sucesso!")
    except KeyboardInterrupt:
        print("\nProcesso interrompido pelo usuário. Progresso salvo.")
    except Exception as e:
        print(f"Erro fatal: {str(e)}. Progresso salvo.")