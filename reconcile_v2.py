import pandas as pd
import numpy as np
import re
import os
import warnings
import itertools

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

f1_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Arise Comercio DMC Recon - July.xlsx'
f2_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Consolidated Payments to Arise Globus Account.xlsx'
output_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Reconciliation_Report_June_July_2nd_Level.xlsx'

print("Loading data...")
df_bank = pd.read_excel(f1_path, sheet_name='Bank Statement')
df_consol = pd.read_excel(f2_path, sheet_name='Consolidated List', skiprows=1)

# Convert dates
df_bank['Parsed_Date'] = pd.to_datetime(df_bank['Post Date'], errors='coerce', format='mixed', dayfirst=True)
df_consol['Parsed_Date'] = pd.to_datetime(df_consol['INFLOW/DATE'], errors='coerce', format='mixed', dayfirst=True)

start_date = pd.to_datetime('2026-06-26')
end_date = pd.to_datetime('2026-07-31')

df_bank = df_bank[(df_bank['Parsed_Date'] >= start_date) & (df_bank['Parsed_Date'] <= end_date)].copy()
df_consol = df_consol[(df_consol['Parsed_Date'] >= start_date) & (df_consol['Parsed_Date'] <= end_date)].copy()

df_bank['Credit'] = pd.to_numeric(df_bank['Credit'], errors='coerce').fillna(0)
df_consol['INFLOWAMOUNT'] = pd.to_numeric(df_consol['INFLOWAMOUNT'], errors='coerce').fillna(0)

df_bank = df_bank[df_bank['Credit'] > 0]
df_consol = df_consol[df_consol['INFLOWAMOUNT'] > 0]

def extract_account(narration):
    if pd.isna(narration): return ""
    matches = re.findall(r'\d{8,}', str(narration))
    if matches: return matches[0].lstrip('0')
    return ""

def clean_account(acc):
    if pd.isna(acc): return ""
    acc_str = str(acc).strip()
    if acc_str.endswith('.0'): acc_str = acc_str[:-2]
    return acc_str.lstrip('0')

df_bank['Extracted_Account'] = df_bank['Narration'].apply(extract_account)
df_consol['Cleaned_Account'] = df_consol['ACCOUNT NUMBER'].apply(clean_account)
df_bank['Narration_Upper'] = df_bank['Narration'].astype(str).str.upper()
df_consol['Sender_Upper'] = df_consol['SENDER NAME'].astype(str).str.upper()

def is_fuzzy_match(sender, narration):
    if not sender or not narration or pd.isna(sender) or pd.isna(narration): 
        return False
    sender_str = str(sender).strip()
    if sender_str in narration:
        return True
    
    words = [w for w in sender_str.split() if len(w) > 3]
    for w in words:
        if w in narration:
            return True
    return False

matched_bank_idx = set()
matched_consol_idx = set()
reconciled_records = []

print("Tier 1: Exact Match...")
for i, bank_row in df_bank.iterrows():
    b_acc = bank_row['Extracted_Account']
    b_amt = bank_row['Credit']
    if not b_acc: continue
        
    match_mask = (df_consol['Cleaned_Account'] == b_acc) & (np.isclose(df_consol['INFLOWAMOUNT'], b_amt)) & (~df_consol.index.isin(matched_consol_idx))
    matches = df_consol[match_mask]
    
    if not matches.empty:
        c_idx = matches.index[0]
        c_row = matches.iloc[0]
        matched_bank_idx.add(i)
        matched_consol_idx.add(c_idx)
        reconciled_records.append({
            'Match_Type': 'Tier 1 (Exact Account + Amount)',
            'Date': bank_row['Post Date'],
            'Bank_Amount': b_amt,
            'Consol_Amount': c_row['INFLOWAMOUNT'],
            'Bank_Narration': bank_row['Narration'],
            'Consol_Sender': c_row['SENDER NAME']
        })

print("Tier 2: Fuzzy Name + Date Match...")
for c_idx, c_row in df_consol.iterrows():
    if c_idx in matched_consol_idx: continue
    c_amt = c_row['INFLOWAMOUNT']
    c_date = c_row['Parsed_Date']
    sender = c_row['Sender_Upper']
    
    # Allow 1 day difference
    date_mask = (df_bank['Parsed_Date'] >= c_date - pd.Timedelta(days=1)) & (df_bank['Parsed_Date'] <= c_date + pd.Timedelta(days=1))
    amt_mask = np.isclose(df_bank['Credit'], c_amt)
    unmatched_mask = ~df_bank.index.isin(matched_bank_idx)
    
    potential_banks = df_bank[date_mask & amt_mask & unmatched_mask]
    
    for b_idx, b_row in potential_banks.iterrows():
        if is_fuzzy_match(sender, b_row['Narration_Upper']):
            matched_bank_idx.add(b_idx)
            matched_consol_idx.add(c_idx)
            reconciled_records.append({
                'Match_Type': 'Tier 2 (Fuzzy Name + Date + Amount)',
                'Date': b_row['Post Date'],
                'Bank_Amount': b_row['Credit'],
                'Consol_Amount': c_amt,
                'Bank_Narration': b_row['Narration'],
                'Consol_Sender': c_row['SENDER NAME']
            })
            break

print("Tier 3: Split Payments Match...")
# We look for multiple bank transactions that sum up to a single consolidated amount on the SAME date with a fuzzy name match
for c_idx, c_row in df_consol.iterrows():
    if c_idx in matched_consol_idx: continue
    c_amt = c_row['INFLOWAMOUNT']
    c_date = c_row['Parsed_Date']
    sender = c_row['Sender_Upper']
    
    # Find all unmatched bank records on same day (+/- 1 day) that match the sender name fuzzily
    date_mask = (df_bank['Parsed_Date'] >= c_date - pd.Timedelta(days=1)) & (df_bank['Parsed_Date'] <= c_date + pd.Timedelta(days=1))
    unmatched_mask = ~df_bank.index.isin(matched_bank_idx)
    
    potential_banks = df_bank[date_mask & unmatched_mask]
    
    # Filter those that fuzzily match name
    valid_banks = []
    for b_idx, b_row in potential_banks.iterrows():
        if is_fuzzy_match(sender, b_row['Narration_Upper']) or (c_row['Cleaned_Account'] and c_row['Cleaned_Account'] == b_row['Extracted_Account']):
            valid_banks.append((b_idx, b_row['Credit'], b_row['Narration'], b_row['Post Date']))
    
    if len(valid_banks) > 1:
        found_combo = False
        # Try combinations up to length 4 to find the sum
        for L in range(2, min(5, len(valid_banks) + 1)):
            for subset in itertools.combinations(valid_banks, L):
                subset_sum = sum(item[1] for item in subset)
                if np.isclose(subset_sum, c_amt):
                    # Match found!
                    matched_consol_idx.add(c_idx)
                    
                    b_narrations = " | ".join([item[2] for item in subset])
                    b_dates = " | ".join([str(item[3]) for item in subset])
                    
                    for item in subset:
                        matched_bank_idx.add(item[0])
                        
                    reconciled_records.append({
                        'Match_Type': 'Tier 3 (Split Payments Sum)',
                        'Date': b_dates,
                        'Bank_Amount': subset_sum,
                        'Consol_Amount': c_amt,
                        'Bank_Narration': b_narrations,
                        'Consol_Sender': c_row['SENDER NAME']
                    })
                    found_combo = True
                    break
            if found_combo: break

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
