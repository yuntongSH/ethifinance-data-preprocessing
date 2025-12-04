import pandas as pd
import os
import re
import sys

# Configuration
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))
CSV_FILENAME = "EthiFinance ESG ratings - Universe - Raw Datas.csv"
EXCEL_FILENAME = "EthiFinance ESG ratings - Universe - Raw Datas.xlsx"

def clean_for_excel(val):
    '''Remove illegal characters that cause Excel errors.'''
    if not isinstance(val, str):
        return val
    # Remove ASCII control characters (0-31) except tab (9), newline (10), carriage return (13)
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', val)

def convert_csv_to_excel():
    """Step 1: Convert CSV to Excel"""
    print("=" * 60)
    print("STEP 1: Converting CSV to Excel")
    print("=" * 60)
    
    # Use the CSV file in the same folder as this script
    csv_path = os.path.join(SCRIPT_FOLDER, CSV_FILENAME)
    
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found: {csv_path}")
        return False

    filename = os.path.basename(csv_path)
    excel_filename = os.path.splitext(filename)[0] + ".xlsx"
    excel_path = os.path.join(SCRIPT_FOLDER, excel_filename)

    print(f"Processing file: {filename}")
    print(f"Full path: {csv_path}")

    df = None
    
    # 1. Try different encodings
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings_to_try:
        print(f"\nTrying encoding: {encoding}...")
        
        # 2. Try different separators for each encoding
        separators = [';', ',', '\t']
        
        for sep in separators:
            try:
                print(f"  Testing separator: '{sep}'")
                # Read first few lines to check structure without loading everything
                df_preview = pd.read_csv(csv_path, sep=sep, encoding=encoding, nrows=5)
                
                if len(df_preview.columns) > 1:
                    print(f"  ✓ Match found! Separator: '{sep}', Encoding: {encoding}")
                    
                    # Load full file with these settings
                    print("  Loading full file...")
                    df = pd.read_csv(csv_path, sep=sep, encoding=encoding, low_memory=False, on_bad_lines='skip')
                    break
            except Exception:
                continue
        
        if df is not None:
            break
    
    if df is None:
        print("❌ Failed to read CSV with standard configurations.")
        print("Trying fallback: default pandas engine with error skipping...")
        try:
            df = pd.read_csv(csv_path, low_memory=False, on_bad_lines='skip')
        except Exception as e:
            print(f"❌ Critical error reading file: {e}")
            return False

    print(f"\n✓ Data Loaded Successfully")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")

    # 3. Clean data for Excel
    print("\nCleaning data for Excel compatibility...")
    # Apply cleaning to all object (string) columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(clean_for_excel)

    # 4. Check Excel limits
    EXCEL_ROW_LIMIT = 1048576
    if len(df) > EXCEL_ROW_LIMIT:
        print(f"⚠️ Warning: Data exceeds Excel row limit ({len(df):,} > {EXCEL_ROW_LIMIT})")
        print(f"  Truncating to first {EXCEL_ROW_LIMIT} rows.")
        df = df.head(EXCEL_ROW_LIMIT)

    # 5. Save
    print(f"Saving to Excel: {excel_path}")
    try:
        df.to_excel(excel_path, index=False, engine='openpyxl')
        file_size_mb = os.path.getsize(excel_path) / (1024 * 1024)
        print(f"\n✅ CSV to Excel conversion complete! ({file_size_mb:.2f} MB)")
        return True
    except Exception as e:
        print(f"❌ Error saving Excel file: {e}")
        return False

def preprocess_data():
    """Step 2: Preprocess the Excel data"""
    print("\n" + "=" * 60)
    print("STEP 2: Preprocessing Data")
    print("=" * 60)
    
    excel_path = os.path.join(SCRIPT_FOLDER, EXCEL_FILENAME)
    
    if not os.path.exists(excel_path):
        print(f"❌ Error: Excel file not found: {excel_path}")
        return False
    
    print(f"Loading Excel file: {EXCEL_FILENAME}")
    
    # Read the Excel file with header starting from row 0 (to keep both row 1 and row 2)
    df = pd.read_excel(excel_path, header=[0, 1])
    
    print(f"Original shape: {df.shape}")
    print(f"Original columns: {len(df.columns)}")
    
    # Step 1: Delete all columns with names ending with .1 or .2
    print("\n1. Deleting columns ending with .1 or .2...")
    columns_to_keep = []
    for col in df.columns:
        # Check if the first level of the column name ends with .1 or .2
        col_name = str(col[0]) if isinstance(col, tuple) else str(col)
        if not (col_name.endswith('.1') or col_name.endswith('.2')):
            columns_to_keep.append(col)
    
    df = df[columns_to_keep]
    print(f"Columns after deletion: {len(df.columns)}")
    
    # Step 2: Delete row 3 (which is index 2 after headers)
    # Since we have multi-level headers, row 3 would be at index 0 of the data
    print("\n2. Deleting row 3 ('Valeur')...")
    df = df.drop(df.index[0])
    print(f"Shape after deleting row 3: {df.shape}")
    
    # Step 3: Row 2 (the code of each indicateur) is already kept as part of the header
    print("\n3. Row 2 (indicator codes) is kept in the header structure")
    
    # Step 4: Delete all lines where 'Campagne' column has no data
    print("\n4. Deleting rows with no data in 'Campagne' column...")
    # Find the 'Campagne' column
    campagne_col = None
    for col in df.columns:
        col_name = str(col[0]) if isinstance(col, tuple) else str(col)
        if 'Campagne' in col_name:
            campagne_col = col
            break
    
    if campagne_col is not None:
        print(f"Found 'Campagne' column: {campagne_col}")
        # Drop rows where Campagne is NaN or empty
        df = df[df[campagne_col].notna() & (df[campagne_col] != '')]
        print(f"Shape after filtering: {df.shape}")
    else:
        print("⚠️ Warning: 'Campagne' column not found")
    
    # Flatten the multi-level column names before saving
    # Combine both levels into a single level
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{col[0]}" if 'Unnamed' in str(col[1]) else f"{col[0]}_{col[1]}" 
                      for col in df.columns]
    
    # Convert values to numbers starting from column N (index 13)
    print("\n5. Converting values to numbers (from column N onwards)...")
    for i, col in enumerate(df.columns):
        if i >= 13:  # Column N is index 13 (A=0, B=1, ..., N=13)
            # Keep "NC" values, convert everything else to numeric
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
        else:
            # For columns before N, try to convert but keep as text if not numeric
            df[col] = pd.to_numeric(df[col], errors='ignore')
    
    # Replace "." with "," in text columns (before column N)
    print("\n6. Replacing '.' with ',' in text columns...")
    for i, col in enumerate(df.columns):
        if i < 13 and df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).replace('.', ',') if pd.notna(x) else x)
    
    # Replace all empty/NaN cells with "NC" (from column N onwards)
    print("\n7. Replacing empty cells with 'NC' (from column N onwards)...")
    for i, col in enumerate(df.columns):
        if i >= 13:
            df[col] = df[col].fillna('NC')
            # Also replace empty strings with "NC"
            df[col] = df[col].replace('', 'NC')
            # Replace "Pas d'information" with "NC"
            df[col] = df[col].replace("Pas d'information", 'NC')
    
    # Calculate new indicators
    print("\n8. Calculating new indicators...")
    
    # Find Q36 and Q35 columns for "Ratio de mixité dans les promotions de managers"
    q36_col = None
    q35_col = None
    q124_col = None
    q410_col = None
    q45_col = None
    q302_col = None
    
    for col in df.columns:
        col_lower = str(col).lower()
        if 'q36' in col_lower:
            q36_col = col
        elif 'q35' in col_lower:
            q35_col = col
        elif 'q124' in col_lower:
            q124_col = col
        elif 'q410' in col_lower:
            q410_col = col
        elif 'q45' in col_lower:
            q45_col = col
        elif 'q302' in col_lower:
            q302_col = col
    
    # Calculate "Ratio de mixité dans les promotions de managers" = Q36/Q35
    if q36_col is not None and q35_col is not None:
        print(f"  Creating 'Ratio de mixité dans les promotions de managers' from {q36_col} / {q35_col}")
        
        def calculate_ratio(row):
            q36_val = row[q36_col]
            q35_val = row[q35_col]
            
            # If either is NC or not numeric, return NC
            if q36_val == 'NC' or q35_val == 'NC':
                return 'NC'
            if pd.isna(q36_val) or pd.isna(q35_val):
                return 'NC'
            
            try:
                q36_num = float(q36_val)
                q35_num = float(q35_val)
                
                # Avoid division by zero
                if q35_num == 0:
                    return 'NC'
                
                return q36_num / q35_num
            except:
                return 'NC'
        
        df['Ratio de mixité dans les promotions de managers'] = df.apply(calculate_ratio, axis=1)
        print("  ✓ Ratio de mixité calculated")
    else:
        print(f"  ⚠️ Warning: Could not find Q36 or Q35 columns (Q36: {q36_col}, Q35: {q35_col})")
    
    # Calculate "Evolution nette de l'effectif" = Q124/Q410
    if q124_col is not None and q410_col is not None:
        print(f"  Creating 'Evolution nette de l'effectif' from {q124_col} / {q410_col}")
        
        def calculate_evolution(row):
            q124_val = row[q124_col]
            q410_val = row[q410_col]
            
            # If either is NC or not numeric, return NC
            if q124_val == 'NC' or q410_val == 'NC':
                return 'NC'
            if pd.isna(q124_val) or pd.isna(q410_val):
                return 'NC'
            
            try:
                q124_num = float(q124_val)
                q410_num = float(q410_val)
                
                # Avoid division by zero
                if q410_num == 0:
                    return 'NC'
                
                return q124_num / q410_num
            except:
                return 'NC'
        
        df['Evolution nette de l\'effectif'] = df.apply(calculate_evolution, axis=1)
        print("  ✓ Evolution nette calculated")
    else:
        print(f"  ⚠️ Warning: Could not find Q124 or Q410 columns (Q124: {q124_col}, Q410: {q410_col})")
    
    # Replace "OUI" by 1 and "NON" by 0 for Q45 and Q302
    print("\n9. Converting OUI/NON to 1/0 for Q45 and Q302...")
    
    if q45_col is not None:
        print(f"  Converting {q45_col}...")
        df[q45_col] = df[q45_col].replace({'OUI': 1, 'NON': 0, 'Oui': 1, 'Non': 0, 'oui': 1, 'non': 0})
        print("  ✓ Q45 converted")
    else:
        print(f"  ⚠️ Warning: Could not find Q45 column")
    
    if q302_col is not None:
        print(f"  Converting {q302_col}...")
        df[q302_col] = df[q302_col].replace({'OUI': 1, 'NON': 0, 'Oui': 1, 'Non': 0, 'oui': 1, 'non': 0})
        print("  ✓ Q302 converted")
    else:
        print(f"  ⚠️ Warning: Could not find Q302 column")
    
    # Save the preprocessed data
    output_filename = "EthiFinance ESG ratings - Universe - Raw Datas - Preprocessed.xlsx"
    output_path = os.path.join(SCRIPT_FOLDER, output_filename)
    
    print(f"\nSaving preprocessed data to: {output_filename}")
    try:
        df.to_excel(output_path, index=False)
        print(f"✅ Preprocessing complete!")
        print(f"Final shape: {df.shape}")
        print(f"Output saved to: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Error saving preprocessed file: {e}")
        return False

def main():
    """Main function to run both conversion and preprocessing"""
    print("\n" + "=" * 60)
    print("ETHIFINANCE DATA PROCESSING PIPELINE")
    print("=" * 60)
    
    # Step 1: Convert CSV to Excel
    if not convert_csv_to_excel():
        print("\n❌ Pipeline failed at CSV conversion step")
        return
    
    # Step 2: Preprocess the data
    if not preprocess_data():
        print("\n❌ Pipeline failed at preprocessing step")
        return
    
    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETE!")
    print("=" * 60)
    print(f"Final output: EthiFinance ESG ratings - Universe - Preprocessed.xlsx")

if __name__ == "__main__":
    main()
