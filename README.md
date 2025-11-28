# EthiFinance Data Preprocessing

This project processes EthiFinance ESG ratings data from CSV format through various preprocessing steps.

## Requirements

- Python 3.x
- pandas
- openpyxl

Install dependencies:
```bash
pip install pandas openpyxl
```

## Usage

1. Place your CSV file named `EthiFinance ESG ratings - Universe - Raw Datas.csv` in the same directory as the script.

2. Run the processing pipeline:
```bash
python process_data.py
```

3. The script will generate `EthiFinance ESG ratings - Universe - Preprocessed.xlsx` with all preprocessing applied.

## Processing Steps

### Step 1: CSV to Excel Conversion
- Automatically detects encoding (UTF-8, Latin-1, CP1252)
- Automatically detects separator (`;`, `,`, `\t`)
- Cleans illegal characters for Excel compatibility
- Saves as `.xlsx` format

### Step 2: Data Preprocessing
1. **Delete columns** ending with `.1` or `.2` (data sources and comments)
2. **Delete row 3** containing "Valeur"
3. **Keep row 2** containing indicator codes in the header
4. **Delete rows** with no data in 'Campagne' column
5. **Convert to numbers** starting from column N onwards (preserving "NC" values)
6. **Replace "." with ","** in text columns (A-M) for regional formatting
7. **Replace empty cells** with "NC" from column N onwards
8. **Replace "Pas d'information"** with "NC"
9. **Calculate new indicators**:
   - "Ratio de mixité dans les promotions de managers" = Q36/Q35
   - "Evolution nette de l'effectif" = Q124/Q410
   - Both return "NC" if either input is "NC" or division by zero
10. **Convert OUI/NON to 1/0** for:
    - Q45: "Existence de dispositifs de partage des bénéfices"
    - Q302: "Réalisation d'enquêtes auprès des salariés"

## Output

The final preprocessed Excel file contains:
- Cleaned and filtered data
- Numeric values properly formatted
- Missing data marked as "NC"
- Two calculated indicator columns
- Binary indicators (0/1) for yes/no questions
