# EthiFinance Data Preprocessing

This project processes EthiFinance ESG data files. There are two separate processing scripts:
- **Raw Data Processing**: Converts and preprocesses raw questionnaire data
- **Ratings Processing**: Merges and processes ESG ratings from multiple years

## Requirements

- Python 3.x
- pandas
- openpyxl

Install dependencies:
```bash
pip install pandas openpyxl
```

## Scripts

### 1. process_data.py - Raw Data Processing
Processes raw ESG questionnaire data from CSV format through multiple preprocessing steps.

**Usage:**
```bash
python process_data.py
```

**Input:** `EthiFinance ESG ratings - Universe - Raw Datas.csv`  
**Output:** `EthiFinance ESG ratings - Universe - Raw Datas - Preprocessed.xlsx`

**Data Type:** Raw questionnaire responses and indicators

### 2. process_ratings.py - Ratings Merger
Merges and processes ESG ratings data from 2024 and 2025 into a consolidated file.

**Usage:**
```bash
python process_ratings.py
```

**Input:** 
- `EthiFinance ESG ratings - Universe - Ratings - 2024.xlsx`
- `EthiFinance ESG ratings - Universe - Ratings - 2025.csv`

**Output:** `EthiFinance ESG ratings - Universe - Ratings - Preprocessed.xlsx`

**Data Type:** ESG ratings and scores

## Processing Steps

### process_data.py - Raw Data Processing Pipeline

Processes raw ESG questionnaire data with comprehensive data cleaning and transformation.

#### Step 1: CSV to Excel Conversion
- **Automatic Format Detection**: Tries multiple encoding formats (UTF-8, Latin-1, CP1252) and separators (`;`, `,`, `\t`)
- **Excel Compatibility**: Removes illegal characters that cause Excel errors
- **Large File Handling**: Validates against Excel's row limit (1,048,576 rows)
- **Output**: Converts CSV to `.xlsx` format

#### Step 2: Data Preprocessing
1. **Column Cleanup**: Deletes columns ending with `.1` or `.2` (data sources and comments)
2. **Row Deletion**: Removes row 3 containing "Valeur" 
3. **Header Management**: Preserves row 2 with indicator codes in multi-level headers
4. **Campaign Filter**: Removes rows with no data in 'Campagne' column
5. **Numeric Conversion**: Converts values from column N onwards to numbers (preserving "NC" values)
6. **Regional Formatting**: Replaces "." with "," in text columns (A-M)
7. **Missing Data**: Fills empty cells with "NC" from column N onwards
8. **Data Standardization**: Replaces "Pas d'information" with "NC"
9. **Calculated Indicators**:
   - **Ratio de mixité dans les promotions de managers** = Q36/Q35
   - **Evolution nette de l'effectif** = Q124/Q410
   - Both return "NC" if either input is "NC" or division by zero occurs
10. **Binary Conversion**: Converts OUI/NON to 1/0 for:
    - Q45: "Existence de dispositifs de partage des bénéfices"
    - Q302: "Réalisation d'enquêtes auprès des salariés"

### process_ratings.py - Ratings Merger Pipeline

Merges ESG ratings from multiple years with intelligent value prioritization.

#### Processing Steps:
1. **File Loading**: 
   - Reads 2024 Excel file and 2025 CSV file
   - Auto-detects CSV encoding and separator
   
2. **Row Cleanup**: Deletes second row from both files

3. **Column Naming**: Adds year suffixes (`_2024` and `_2025`) to all columns except ISIN

4. **Data Merging**: Performs outer join on ISIN to include all records from both years

5. **Missing Data**: Fills all empty cells with "NC"

6. **Final Columns Creation**:
   - **Note Générale préliminaire_final**: Uses 2025 value if available, otherwise falls back to 2024
   - **Note SOCIAL_final**: Uses 2025 "Note SOCIAL - RESSOURCES HUMAINES" if available, otherwise 2024 value
   - **Campagne_final**: Indicates source year ("2025", "2024", or "NC" if both years are "NC")

7. **Data Validation**: Removes rows where ISIN column contains "NC"

## Output Files

### process_data.py Output
**File:** `EthiFinance ESG ratings - Universe - Raw Datas - Preprocessed.xlsx`

Contains processed raw questionnaire data:
- Cleaned and filtered indicator data
- Numeric values properly formatted
- Missing data standardized as "NC"
- Two calculated indicator columns (ratio and evolution)
- Binary indicators (0/1) for yes/no questions
- Regional formatting applied

### process_ratings.py Output
**File:** `EthiFinance ESG ratings - Universe - Ratings - Preprocessed.xlsx`

Contains merged ratings data:
- All columns from both 2024 and 2025 with year suffixes
- Three consolidated final columns for ratings and campaign tracking
- All empty cells filled with "NC"
- Only valid ISIN records (invalid rows removed)
- Prioritizes most recent (2025) ratings when available
