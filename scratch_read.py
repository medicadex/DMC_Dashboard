import pandas as pd
import os

f1 = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Arise Comercio DMC Recon - July.xlsx'
f2 = 'C:/Users/Qadebowale/Desktop/SQL RECOVERY PROJECT/Account reconciliation/Consolidated Payments to Arise Globus Account.xlsx'

print('=== FILE 1 ===')
xls1 = pd.ExcelFile(f1)
print('Sheets:', xls1.sheet_names)
for s in xls1.sheet_names:
    print(f'\n--- Sheet: {s} ---')
    df = pd.read_excel(xls1, sheet_name=s)
    print(df.head(3))
    print('Columns:', df.columns.tolist())

print('\n=== FILE 2 ===')
xls2 = pd.ExcelFile(f2)
print('Sheets:', xls2.sheet_names)
for s in xls2.sheet_names:
    print(f'\n--- Sheet: {s} ---')
    df = pd.read_excel(xls2, sheet_name=s)
    print(df.head(3))
    print('Columns:', df.columns.tolist())
