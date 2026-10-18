import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

input_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Reconciliation_Report_June_July_2nd_Level.xlsx'

print("Loading existing reconciliation data...")
df_recon = pd.read_excel(input_path, sheet_name='Reconciled_Matches')
df_un_bank = pd.read_excel(input_path, sheet_name='Unreconciled_Bank')
df_mis_consol = pd.read_excel(input_path, sheet_name='Missing_Consolidated')

# We want to build a summary table:
# SENDER NAME | Comercio (Consol) | Bank | Variance | Comment

summary_data = []

# 1. Process Reconciled Matches
for _, row in df_recon.iterrows():
    sender = row.get('Consol_Sender', 'Unknown')
    # Use Consol_Amount and Bank_Amount from Tier 2/3 output, or Amount if it was Tier 1 early
    c_amt = row.get('Consol_Amount', row.get('Amount', 0))
    b_amt = row.get('Bank_Amount', row.get('Amount', 0))
    
    summary_data.append({
        'SENDER NAME': sender,
        'Comercio': c_amt,
        'Bank': b_amt,
        'Variance': float(c_amt) - float(b_amt),
        'Comment': 'Reconciled'
    })

# 2. Process Missing Consolidated (Not in Bank)
for _, row in df_mis_consol.iterrows():
    sender = row.get('SENDER NAME', 'Unknown')
    c_amt = row.get('INFLOWAMOUNT', 0)
    b_amt = 0
    
    summary_data.append({
        'SENDER NAME': sender,
        'Comercio': c_amt,
        'Bank': b_amt,
        'Variance': float(c_amt) - float(b_amt),
        'Comment': 'Not in Bank'
    })

# 3. Process Unreconciled Bank Receipts (Not in DMC / Unidentified)
for _, row in df_un_bank.iterrows():
    narration = str(row.get('Narration', '')).strip()
    sender = narration if narration else 'No Name'
    c_amt = 0
    b_amt = row.get('Credit', 0)
    
    # Determine if unidentified
    if sender == 'No Name' or len(sender) < 3 or pd.isna(row.get('Narration')):
        comment = 'Unidentified Transaction'
        sender = 'No Name'
    else:
        comment = 'Not in DMC'
        
    summary_data.append({
        'SENDER NAME': sender,
        'Comercio': c_amt,
        'Bank': b_amt,
        'Variance': float(c_amt) - float(b_amt),
        'Comment': comment
    })

# Group by Sender Name to get totals (optional, but makes it cleaner)
df_summary = pd.DataFrame(summary_data)

# Aggregate if there are multiple unmatched for the same sender
df_grouped = df_summary.groupby(['SENDER NAME', 'Comment']).agg({
    'Comercio': 'sum',
    'Bank': 'sum',
    'Variance': 'sum'
}).reset_index()

# Reorder columns
df_grouped = df_grouped[['SENDER NAME', 'Comercio', 'Bank', 'Variance', 'Comment']]

# Calculate Totals row
totals = pd.DataFrame([{
    'SENDER NAME': 'TOTAL',
    'Comercio': df_grouped['Comercio'].sum(),
    'Bank': df_grouped['Bank'].sum(),
    'Variance': df_grouped['Variance'].sum(),
    'Comment': ''
}])

df_final = pd.concat([totals, df_grouped], ignore_index=True)

output_path = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Recon_Summary.xlsx'

print("Creating Recon_Summary.xlsx...")
df_final.to_excel(output_path, sheet_name='Recon_Summary', index=False)

print("Done! Recon_Summary.xlsx created.")
