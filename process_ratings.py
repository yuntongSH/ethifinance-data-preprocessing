import pandas as pd
import os

# Configuration
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))
FILE_2024 = "EthiFinance ESG ratings - Universe - Ratings - 2024.xlsx"
FILE_2025 = "EthiFinance ESG ratings - Universe - Ratings - 2025.csv"
OUTPUT_FILE = "EthiFinance ESG ratings - Universe - Ratings - Preprocessed.xlsx"

def process_ratings():
    """
    Process and merge ESG rating files from 2024 and 2025.
    
    Steps:
    1. Load both files
    2. Delete the second row from both files
    3. Add year suffixes to column names (_2024 and _2025)
    4. Merge files based on ISIN
    5. Save the result as an Excel file
    """
    print("=" * 60)
    print("ESG RATINGS DATA PROCESSING")
    print("=" * 60)
    
    # File paths
    file_2024_path = os.path.join(SCRIPT_FOLDER, FILE_2024)
    file_2025_path = os.path.join(SCRIPT_FOLDER, FILE_2025)
    output_path = os.path.join(SCRIPT_FOLDER, OUTPUT_FILE)
    
    # Check if files exist
    if not os.path.exists(file_2024_path):
        print(f"❌ Error: File not found: {file_2024_path}")
        return False
    
    if not os.path.exists(file_2025_path):
        print(f"❌ Error: File not found: {file_2025_path}")
        return False
    
    print(f"\n📂 Loading files...")
    
    # Load 2024 file (Excel)
    print(f"   Loading {FILE_2024}...")
    df_2024 = pd.read_excel(file_2024_path)
    print(f"   ✓ Loaded with shape: {df_2024.shape}")
    
    # Load 2025 file (CSV)
    print(f"   Loading {FILE_2025}...")
    # Try different encodings and separators
    df_2025 = None
    encodings = ['utf-8', 'latin-1', 'cp1252']
    separators = [',', ';', '\t']
    
    for encoding in encodings:
        for sep in separators:
            try:
                df_2025 = pd.read_csv(file_2025_path, encoding=encoding, sep=sep)
                if df_2025.shape[1] > 1:  # Valid if more than 1 column
                    print(f"   ✓ Loaded with shape: {df_2025.shape} (encoding: {encoding}, separator: '{sep}')")
                    break
            except:
                continue
        if df_2025 is not None and df_2025.shape[1] > 1:
            break
    
    if df_2025 is None or df_2025.shape[1] <= 1:
        print(f"❌ Error: Could not load {FILE_2025} with proper formatting")
        return False
    
    # Step 1: Delete the second row (index 1)
    print(f"\n🗑️  Deleting second row from both files...")
    if len(df_2024) > 1:
        df_2024 = df_2024.drop(index=1).reset_index(drop=True)
        print(f"   ✓ 2024 file: Second row deleted, new shape: {df_2024.shape}")
    
    if len(df_2025) > 1:
        df_2025 = df_2025.drop(index=1).reset_index(drop=True)
        print(f"   ✓ 2025 file: Second row deleted, new shape: {df_2025.shape}")
    
    # Step 2: Add year suffixes to column names
    print(f"\n🏷️  Adding year suffixes to column names...")
    
    # For 2024 file
    df_2024.columns = [col if col == 'ISIN' else f"{col}_2024" for col in df_2024.columns]
    print(f"   ✓ 2024 columns renamed (keeping ISIN as is)")
    
    # For 2025 file
    df_2025.columns = [col if col == 'ISIN' else f"{col}_2025" for col in df_2025.columns]
    print(f"   ✓ 2025 columns renamed (keeping ISIN as is)")
    
    # Step 3: Merge files based on ISIN
    print(f"\n🔗 Merging files based on ISIN...")
    
    # Check if ISIN column exists in both files
    if 'ISIN' not in df_2024.columns:
        print(f"❌ Error: 'ISIN' column not found in 2024 file")
        print(f"   Available columns: {list(df_2024.columns)}")
        return False
    
    if 'ISIN' not in df_2025.columns:
        print(f"❌ Error: 'ISIN' column not found in 2025 file")
        print(f"   Available columns: {list(df_2025.columns)}")
        return False
    
    # Merge using outer join to keep all records
    df_merged = pd.merge(df_2025, df_2024, on='ISIN', how='outer')
    print(f"   ✓ Files merged successfully")
    print(f"   ✓ Merged shape: {df_merged.shape}")
    print(f"   ✓ Total unique ISINs: {df_merged['ISIN'].nunique()}")
    
    # Fill empty cells with "NC"
    print(f"\n📝 Filling empty cells with 'NC'...")
    df_merged = df_merged.fillna('NC')
    print(f"   ✓ All empty cells filled with 'NC'")
    
    # Create final columns
    print(f"\n🎯 Creating final columns...")
    
    # Note Générale préliminaire_final
    col_generale_2025 = "Note Générale préliminaire_2025"
    col_generale_2024 = "Note Générale préliminaire_2024"
    
    if col_generale_2025 in df_merged.columns and col_generale_2024 in df_merged.columns:
        df_merged['Note Générale préliminaire_final'] = df_merged.apply(
            lambda row: row[col_generale_2025] if row[col_generale_2025] != 'NC' else row[col_generale_2024],
            axis=1
        )
        print(f"   ✓ 'Note Générale préliminaire_final' column created")
    else:
        print(f"   ⚠️  Warning: Could not create 'Note Générale préliminaire_final'")
    
    # Note SOCIAL_final
    col_social_2025 = "Note SOCIAL - RESSOURCES HUMAINES_2025"
    col_social_2024 = "Note SOCIAL - RESSOURCES HUMAINES_2024"
    
    if col_social_2025 in df_merged.columns and col_social_2024 in df_merged.columns:
        df_merged['Note SOCIAL_final'] = df_merged.apply(
            lambda row: row[col_social_2025] if row[col_social_2025] != 'NC' else row[col_social_2024],
            axis=1
        )
        print(f"   ✓ 'Note SOCIAL_final' column created")
    else:
        print(f"   ⚠️  Warning: Could not create 'Note SOCIAL_final'")
    
    # Campagne_final (based on which year the final value came from)
    if col_generale_2025 in df_merged.columns and col_generale_2024 in df_merged.columns:
        df_merged['Campagne_final'] = df_merged.apply(
            lambda row: '2025' if row[col_generale_2025] != 'NC' 
                       else ('2024' if row[col_generale_2024] != 'NC' else 'NC'),
            axis=1
        )
        print(f"   ✓ 'Campagne_final' column created")
    else:
        print(f"   ⚠️  Warning: Could not create 'Campagne_final'")
    
    # Delete rows where Column A (first column, ISIN) has "NC"
    print(f"\n🗑️  Removing rows with 'NC' in ISIN column...")
    initial_rows = len(df_merged)
    df_merged = df_merged[df_merged['ISIN'] != 'NC']
    removed_rows = initial_rows - len(df_merged)
    print(f"   ✓ Removed {removed_rows} rows with 'NC' in ISIN")
    print(f"   ✓ Remaining rows: {len(df_merged)}")
    
    # Step 4: Save to Excel
    print(f"\n💾 Saving merged file...")
    try:
        df_merged.to_excel(output_path, index=False, engine='openpyxl')
        print(f"   ✓ File saved successfully: {OUTPUT_FILE}")
        print(f"   ✓ Full path: {output_path}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        return False
    
    # Summary
    print(f"\n{'=' * 60}")
    print(f"✅ PROCESSING COMPLETE")
    print(f"{'=' * 60}")
    print(f"   Input files:")
    print(f"     - {FILE_2024} ({df_2024.shape[0]} rows, {df_2024.shape[1]} columns)")
    print(f"     - {FILE_2025} ({df_2025.shape[0]} rows, {df_2025.shape[1]} columns)")
    print(f"   Output file:")
    print(f"     - {OUTPUT_FILE} ({df_merged.shape[0]} rows, {df_merged.shape[1]} columns)")
    print(f"{'=' * 60}")
    
    return True

if __name__ == "__main__":
    success = process_ratings()
    if not success:
        exit(1)
