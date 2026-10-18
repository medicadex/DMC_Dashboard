import pandas as pd
import numpy as np
import re
import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# File paths
f1_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Arise Comercio DMC Recon - July.xlsx'
f2_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Consolidated Payments to Arise Globus Account.xlsx'
output_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Reconciliation_Report_June_July.xlsx'

print("Loading data...")
df_bank = pd.read_excel(f1_path, sheet_name='Bank Statement')
df_consol = pd.read_excel(f2_path, sheet_name='Consolidated List', skiprows=1)

print("Original Bank Statement rows:", len(df_bank))
print("Original Consolidated List rows:", len(df_consol))

# --- Date Filtering ---
print("Filtering dates...")
# Convert dates to datetime objects
df_bank['Parsed_Date'] = pd.to_datetime(df_bank['Post Date'], errors='coerce', format='mixed', dayfirst=True)
df_consol['Parsed_Date'] = pd.to_datetime(df_consol['INFLOW/DATE'], errors='coerce', format='mixed', dayfirst=True)

# Define date range
start_date = pd.to_datetime('2026-06-26')
end_date = pd.to_datetime('2026-07-31')

# Filter
df_bank = df_bank[(df_bank['Parsed_Date'] >= start_date) & (df_bank['Parsed_Date'] <= end_date)].copy()
df_consol = df_consol[(df_consol['Parsed_Date'] >= start_date) & (df_consol['Parsed_Date'] <= end_date)].copy()

print(f"Filtered Bank Statement rows (June 26 - July 31): {len(df_bank)}")
print(f"Filtered Consolidated List rows (June 26 - July 31): {len(df_consol)}")

# --- Data Preparation & Cleaning ---
print("Preparing data...")
df_bank['Credit'] = pd.to_numeric(df_bank['Credit'], errors='coerce').fillna(0)
df_consol['INFLOWAMOUNT'] = pd.to_numeric(df_consol['INFLOWAMOUNT'], errors='coerce').fillna(0)

# Filter out zero amounts
df_bank = df_bank[df_bank['Credit'] > 0]
df_consol = df_consol[df_consol['INFLOWAMOUNT'] > 0]

def extract_account(narration):
    if pd.isna(narration): return ""
    matches = re.findall(r'\d{8,}', str(narration))
    if matches:
        return matches[0].lstrip('0')
    return ""

df_bank['Extracted_Account'] = df_bank['Narration'].apply(extract_account)

def clean_account(acc):
    if pd.isna(acc): return ""
    acc_str = str(acc).strip()
    if acc_str.endswith('.0'):
        acc_str = acc_str[:-2]
    return acc_str.lstrip('0')

df_consol['Cleaned_Account'] = df_consol['ACCOUNT NUMBER'].apply(clean_account)

df_bank['Narration_Upper'] = df_bank['Narration'].astype(str).str.upper()
df_consol['Sender_Upper'] = df_consol['SENDER NAME'].astype(str).str.upper()

matched_bank_idx = set()
matched_consol_idx = set()
reconciled_records = []

# --- Matching Engine ---
print("Running Matching Engine...")

# Tier 1: Exact Match (Account Number + Amount)
for i, bank_row in df_bank.iterrows():
    b_acc = bank_row['Extracted_Account']
    b_amt = bank_row['Credit']
    
    if not b_acc:
        continue
        
    match_mask = (df_consol['Cleaned_Account'] == b_acc) & (np.isclose(df_consol['INFLOWAMOUNT'], b_amt)) & (~df_consol.index.isin(matched_consol_idx))
    matches = df_consol[match_mask]
    
    if not matches.empty:
        c_idx = matches.index[0]
        c_row = matches.iloc[0]
        
        matched_bank_idx.add(i)
        matched_consol_idx.add(c_idx)
        
        reconciled_records.append({
            'Match_Type': 'Tier 1 (Exact Account)',
            'Date': bank_row['Post Date'],
            'Amount': b_amt,
            'Bank_Narration': bank_row['Narration'],
            'Consol_Sender': c_row['SENDER NAME'],
            'Account_Used': b_acc
        })

# Tier 2: Fuzzy Name Match
for i, bank_row in df_bank.iterrows():
    if i in matched_bank_idx:
        continue
        
    b_amt = bank_row['Credit']
    b_narration = bank_row['Narration_Upper']
    
    match_mask = (np.isclose(df_consol['INFLOWAMOUNT'], b_amt)) & (~df_consol.index.isin(matched_consol_idx))
    potential_matches = df_consol[match_mask]
    
    for c_idx, c_row in potential_matches.iterrows():
        sender = str(c_row['Sender_Upper']).strip()
        if sender and len(sender) > 3 and sender in b_narration:
            matched_bank_idx.add(i)
            matched_consol_idx.add(c_idx)
            
            reconciled_records.append({
                'Match_Type': 'Tier 2 (Fuzzy Name)',
                'Date': bank_row['Post Date'],
                'Amount': b_amt,
                'Bank_Narration': bank_row['Narration'],
                'Consol_Sender': c_row['SENDER NAME'],
                'Account_Used': 'Matched by Name'
            })
            break

# --- Generate Output ---
print("Generating Output files...")
reconciled_df = pd.DataFrame(reconciled_records)

unmatched_bank = df_bank.loc[~df_bank.index.isin(matched_bank_idx)].copy()
unmatched_consol = df_consol.loc[~df_consol.index.isin(matched_consol_idx)].copy()

unmatched_bank.drop(columns=['Parsed_Date', 'Extracted_Account', 'Narration_Upper'], inplace=True, errors='ignore')
unmatched_consol.drop(columns=['Parsed_Date', 'Cleaned_Account', 'Sender_Upper'], inplace=True, errors='ignore')

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    if not reconciled_df.empty:
        reconciled_df.to_excel(writer, sheet_name='Reconciled_Matches', index=False)
    else:
        pd.DataFrame({'Message': ['No matches found']}).to_excel(writer, sheet_name='Reconciled_Matches', index=False)
    unmatched_bank.to_excel(writer, sheet_name='Unreconciled_Bank', index=False)
    unmatched_consol.to_excel(writer, sheet_name='Missing_Consolidated', index=False)

print(f"Reconciliation Complete!")
print(f"Total Reconciled: {len(reconciled_df)}")
print(f"Unreconciled Bank Receipts: {len(unmatched_bank)}")
print(f"Missing Consolidated Deposits: {len(unmatched_consol)}")
print(f"Report saved to: {output_path}")
