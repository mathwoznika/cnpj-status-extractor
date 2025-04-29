import pandas as pd

def extract_cnpjs_from_excel(file_path: str) -> list[str]:
    """Extracts CNPJs from a 'cnpj' column in an Excel file.

    Args:
        file_path (str): Path to Excel file (.xlsx or .xls).

    Raises:
        ValueError: If no 'cnpj' column exists in the file.

    Returns:
        list[str]: List of CNPJs as strings.
    """
    
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.lower()
    if 'cnpj' not in df.columns:
        raise ValueError("The file must contain a 'cnpj' column")
    return df['cnpj'].tolist()