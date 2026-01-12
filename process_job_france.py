import pandas as pd
import os
import re

# Configuration
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))
FILE_2023 = "EthiFinance ESG ratings - Universe - Raw Datas -  2023.xlsx"
FILE_2024 = "EthiFinance ESG ratings - Universe - Raw Datas -  2024.xlsx"
OUTPUT_FILE = "EthiFinance ESG ratings - Universe - Job in France.xlsx"

def clean_for_excel(val):
    '''Remove illegal characters that cause Excel errors.'''
    if not isinstance(val, str):
        return val
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', val)

def load_and_preprocess_file(filepath, year_label):
    """Load a file (CSV or Excel) and apply the same preprocessing as process_data.py"""
    print(f"\n{'='*60}")
    print(f"Loading and preprocessing {year_label} data...")
    print(f"{'='*60}")
    
    if not os.path.exists(filepath):
        print(f"❌ Error: File not found: {filepath}")
        return None
    
    df = None
    
    # Check if file is Excel or CSV
    if filepath.endswith('.xlsx') or filepath.endswith('.xls'):
        # Load Excel file
        try:
            print(f"   Loading Excel file...")
            df = pd.read_excel(filepath, header=[0, 1])
            print(f"   ✓ Loaded Excel file with shape: {df.shape}")
        except Exception as e:
            print(f"❌ Error loading Excel file: {e}")
            return None
    else:
        # Try different encodings and separators for CSV
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252']
        separators = [';', ',', '\t']
        
        for encoding in encodings_to_try:
            for sep in separators:
                try:
                    df_preview = pd.read_csv(filepath, sep=sep, encoding=encoding, nrows=5, header=[0, 1])
                    if len(df_preview.columns) > 1:
                        print(f"   ✓ Match found! Separator: '{sep}', Encoding: {encoding}")
                        df = pd.read_csv(filepath, sep=sep, encoding=encoding, low_memory=False, 
                                        on_bad_lines='skip', header=[0, 1])
                        break
                except Exception:
                    continue
            if df is not None:
                break
        
        if df is None:
            print(f"❌ Error: Could not load {filepath}")
            return None
        
        print(f"   ✓ Loaded CSV with shape: {df.shape}")
    
    # Clean data for Excel
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(clean_for_excel)
    
    # Step 1: Delete all columns with names ending with .1 or .2
    print(f"   Deleting columns ending with .1 or .2...")
    columns_to_keep = []
    for col in df.columns:
        col_name = str(col[0]) if isinstance(col, tuple) else str(col)
        if not (col_name.endswith('.1') or col_name.endswith('.2')):
            columns_to_keep.append(col)
    df = df[columns_to_keep]
    
    # Step 2: Delete row 3 (index 0 of data after headers)
    print(f"   Deleting row 3 ('Valeur')...")
    if len(df) > 0:
        df = df.drop(df.index[0])
    
    # Step 3: Delete all lines where 'Campagne' column has no data
    print(f"   Filtering rows with valid 'Campagne'...")
    campagne_col = None
    for col in df.columns:
        col_name = str(col[0]) if isinstance(col, tuple) else str(col)
        if 'Campagne' in col_name:
            campagne_col = col
            break
    
    if campagne_col is not None:
        df = df[df[campagne_col].notna() & (df[campagne_col] != '')]
    
    # Flatten the multi-level column names
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{col[0]}" if 'Unnamed' in str(col[1]) else f"{col[0]}_{col[1]}" 
                      for col in df.columns]
    
    # Convert values to numbers starting from column N (index 13)
    print(f"   Converting values to numbers...")
    for i, col in enumerate(df.columns):
        if i >= 13:
            def convert_with_nc(x):
                if pd.isna(x):
                    return x
                if str(x).strip().upper() == 'NC':
                    return 'NC'
                try:
                    return pd.to_numeric(x)
                except:
                    return x
            df[col] = df[col].apply(convert_with_nc)
    
    # Replace empty/NaN cells with "NC" (from column N onwards)
    print(f"   Replacing empty cells with 'NC'...")
    for i, col in enumerate(df.columns):
        if i >= 13:
            df[col] = df[col].fillna('NC')
            df[col] = df[col].replace('', 'NC')
            df[col] = df[col].replace("Pas d'information", 'NC')
    
    print(f"   ✓ Preprocessing complete. Final shape: {df.shape}")
    return df

def find_column(df, search_terms):
    """Find a column that contains all search terms (case-insensitive)"""
    for col in df.columns:
        col_lower = str(col).lower()
        if all(term.lower() in col_lower for term in search_terms):
            return col
    return None

def process_job_france():
    """
    Process job growth rate in France for companies.
    """
    print("=" * 60)
    print("JOB IN FRANCE - DATA PROCESSING")
    print("=" * 60)
    
    # File paths
    file_2023_path = os.path.join(SCRIPT_FOLDER, FILE_2023)
    file_2024_path = os.path.join(SCRIPT_FOLDER, FILE_2024)
    output_path = os.path.join(SCRIPT_FOLDER, OUTPUT_FILE)
    
    # Load and preprocess both files
    df_2023 = load_and_preprocess_file(file_2023_path, "2023")
    df_2024 = load_and_preprocess_file(file_2024_path, "2024")
    
    if df_2023 is None or df_2024 is None:
        print("❌ Error: Could not load one or both input files")
        return False
    
    # Find required columns
    print(f"\n{'='*60}")
    print("Identifying required columns...")
    print(f"{'='*60}")
    
    # Find ISIN column
    isin_col_2023 = find_column(df_2023, ['isin'])
    isin_col_2024 = find_column(df_2024, ['isin'])
    
    # Find Nom Société column
    nom_col_2024 = find_column(df_2024, ['nom', 'société']) or find_column(df_2024, ['nom société'])
    if nom_col_2024 is None:
        nom_col_2024 = find_column(df_2024, ['nom'])
    
    # Find Pays column
    pays_col_2024 = find_column(df_2024, ['pays'])
    
    # Find Q608 columns (Part de l'effectif total situé dans le pays du siège social)
    q608_col_2023 = find_column(df_2023, ['q608'])
    q608_col_2024 = find_column(df_2024, ['q608'])
    
    # Find Q410 columns (Effectif total en fin d'exercice)
    q410_col_2023 = find_column(df_2023, ['q410'])
    q410_col_2024 = find_column(df_2024, ['q410'])
    
    print(f"   ISIN 2023: {isin_col_2023}")
    print(f"   ISIN 2024: {isin_col_2024}")
    print(f"   Nom Société 2024: {nom_col_2024}")
    print(f"   Pays 2024: {pays_col_2024}")
    print(f"   Q608 2023: {q608_col_2023}")
    print(f"   Q608 2024: {q608_col_2024}")
    print(f"   Q410 2023: {q410_col_2023}")
    print(f"   Q410 2024: {q410_col_2024}")
    
    # Validate required columns exist
    missing_cols = []
    if isin_col_2023 is None: missing_cols.append("ISIN 2023")
    if isin_col_2024 is None: missing_cols.append("ISIN 2024")
    if q608_col_2023 is None: missing_cols.append("Q608 2023")
    if q608_col_2024 is None: missing_cols.append("Q608 2024")
    if q410_col_2023 is None: missing_cols.append("Q410 2023")
    if q410_col_2024 is None: missing_cols.append("Q410 2024")
    
    if missing_cols:
        print(f"❌ Error: Missing required columns: {missing_cols}")
        return False
    
    # Create result dataframe based on 2024 companies
    print(f"\n{'='*60}")
    print("Creating result dataframe...")
    print(f"{'='*60}")
    
    # Start with 2024 data
    result_df = pd.DataFrame()
    result_df['ISIN'] = df_2024[isin_col_2024].values
    
    if nom_col_2024:
        result_df['Nom Société'] = df_2024[nom_col_2024].values
    else:
        result_df['Nom Société'] = 'NC'
    
    if pays_col_2024:
        result_df['Pays'] = df_2024[pays_col_2024].values
    else:
        result_df['Pays'] = 'NC'
    
    # Add 2024 data
    result_df['Q608_2024'] = df_2024[q608_col_2024].values
    result_df['Q410_2024'] = df_2024[q410_col_2024].values
    
    # Merge 2023 data based on ISIN
    df_2023_subset = df_2023[[isin_col_2023, q608_col_2023, q410_col_2023]].copy()
    df_2023_subset.columns = ['ISIN', 'Q608_2023', 'Q410_2023']
    
    result_df = result_df.merge(df_2023_subset, on='ISIN', how='left')
    
    # Fill missing 2023 data with NC
    result_df['Q608_2023'] = result_df['Q608_2023'].fillna('NC')
    result_df['Q410_2023'] = result_df['Q410_2023'].fillna('NC')
    
    print(f"   ✓ Result dataframe created with {len(result_df)} companies")
    
    # Calculate "Emplois en France" for both years
    print(f"\n{'='*60}")
    print("Calculating 'Emplois en France'...")
    print(f"{'='*60}")
    
    def calculate_emplois_france(q410, q608):
        """Calculate Q410 * Q608 / 100"""
        if q410 == 'NC' or q608 == 'NC':
            return 'NC'
        if pd.isna(q410) or pd.isna(q608):
            return 'NC'
        try:
            q410_num = float(q410)
            q608_num = float(q608)
            return q410_num * q608_num / 100
        except:
            return 'NC'
    
    result_df['Emplois en France_2023'] = result_df.apply(
        lambda row: calculate_emplois_france(row['Q410_2023'], row['Q608_2023']), axis=1
    )
    result_df['Emplois en France_2024'] = result_df.apply(
        lambda row: calculate_emplois_france(row['Q410_2024'], row['Q608_2024']), axis=1
    )
    
    print(f"   ✓ 'Emplois en France_2023' calculated")
    print(f"   ✓ 'Emplois en France_2024' calculated")
    
    # Calculate "Evolution d'emploi en France"
    print(f"\n{'='*60}")
    print("Calculating 'Evolution d'emploi en France'...")
    print(f"{'='*60}")
    
    def calculate_evolution(emplois_2024, emplois_2023):
        """Calculate (Emplois en France_2024 / Emplois en France_2023) - 1"""
        if emplois_2024 == 'NC' or emplois_2023 == 'NC':
            return 'NC'
        if pd.isna(emplois_2024) or pd.isna(emplois_2023):
            return 'NC'
        try:
            emp_2024 = float(emplois_2024)
            emp_2023 = float(emplois_2023)
            if emp_2023 == 0:
                return 'NC'
            return (emp_2024 / emp_2023) - 1
        except:
            return 'NC'
    
    result_df["Evolution d'emploi en France"] = result_df.apply(
        lambda row: calculate_evolution(row['Emplois en France_2024'], row['Emplois en France_2023']), axis=1
    )
    
    print(f"   ✓ 'Evolution d'emploi en France' calculated")
    
    # Rename columns for clarity
    result_df = result_df.rename(columns={
        'Q608_2023': 'Part de l\'effectif total situé dans le pays du siège social_Q608_2023',
        'Q410_2023': 'Effectif total en fin d\'exercice_Q410_2023',
        'Q608_2024': 'Part de l\'effectif total situé dans le pays du siège social_Q608_2024',
        'Q410_2024': 'Effectif total en fin d\'exercice_Q410_2024'
    })
    
    # Reorder columns
    column_order = [
        'ISIN',
        'Nom Société',
        'Pays',
        'Part de l\'effectif total situé dans le pays du siège social_Q608_2023',
        'Effectif total en fin d\'exercice_Q410_2023',
        'Part de l\'effectif total situé dans le pays du siège social_Q608_2024',
        'Effectif total en fin d\'exercice_Q410_2024',
        'Emplois en France_2023',
        'Emplois en France_2024',
        "Evolution d'emploi en France"
    ]
    result_df = result_df[column_order]
    
    # Save to Excel
    print(f"\n{'='*60}")
    print("Saving output file...")
    print(f"{'='*60}")
    
    try:
        result_df.to_excel(output_path, index=False, engine='openpyxl')
        print(f"   ✓ File saved: {OUTPUT_FILE}")
        print(f"   ✓ Full path: {output_path}")
        print(f"   ✓ Total companies: {len(result_df)}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        return False
    
    # Summary
    print(f"\n{'='*60}")
    print("✅ PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"   Input files:")
    print(f"     - {FILE_2023}")
    print(f"     - {FILE_2024}")
    print(f"   Output file:")
    print(f"     - {OUTPUT_FILE} ({len(result_df)} rows, {len(result_df.columns)} columns)")
    
    # Show some statistics
    valid_evolution = result_df[result_df["Evolution d'emploi en France"] != 'NC']["Evolution d'emploi en France"]
    if len(valid_evolution) > 0:
        valid_evolution = pd.to_numeric(valid_evolution, errors='coerce')
        print(f"\n   Statistics for 'Evolution d'emploi en France':")
        print(f"     - Valid calculations: {len(valid_evolution.dropna())}")
        print(f"     - Mean: {valid_evolution.mean():.4f}")
        print(f"     - Min: {valid_evolution.min():.4f}")
        print(f"     - Max: {valid_evolution.max():.4f}")
    
    print(f"{'='*60}")
    
    return True

if __name__ == "__main__":
    success = process_job_france()
    if not success:
        exit(1)
