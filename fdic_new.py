import os
import camelot
import numpy as np
import pandas as pd
dict_fetch = {"Past-Due and Nonaccrual Loans / Total Loans" : "Asset Quality",
              "Noncurrent Loans / Total Loans" : "Asset Quality",
              "Loan and Lease Allowance / Total Loans" : "Asset Quality",
              "Loan and Lease Allowance / Noncurrent Loans" : "Asset Quality",
              "Net Loan Losses / Total Loans" : "Asset Quality",
              "Return on Assets" : "Earnings",
              "Net Interest Margin" : "Earnings",
              "Yield on Earning Assets" : "Earnings",
              "Cost of Funding Earning Assets" : "Earnings",
              "Provisions to Avg. Assets" : "Earnings",
              "Overhead to Avg. Assets" : "Earnings",
              "Net Loans to Assets" : "Liquidity",
              "Long-term Assets to Assets" : "Liquidity"
              }
files_path = 'D:\\BYTEIQ\\FDIC_DOWNLOAD\\'
# files_path = 'D:\\BYTEIQ\\fdic\\fdic\\download\\'
lf_paths = []
download_list = os.listdir(files_path)
download_list.sort()
def extract(file_path):
    try:
        tables = camelot.read_pdf(filepath=file_path,flavor='stream',edge_tol=1500)
        temp_df = tables[1].df
        remove_from_bracket = lambda text : text.split('(', 1)[0]
        remove_spaces = lambda text : text.strip()
        temp_df[0] = temp_df[0].map(remove_from_bracket).map(remove_spaces)
        temp_df = temp_df[0:52]
        temp_df = temp_df.drop([1], axis=1)
        column_name_1 = temp_df[0][1]
        temp_df = temp_df.replace(r'^\s*$', np.nan, regex=True)
        temp_df = temp_df.dropna()
        temp_df = temp_df.reset_index(drop = True)
        temp_df.columns = temp_df.iloc[0]
        temp_df = temp_df.drop(temp_df.index[0])
        temp_df.rename(columns={'Employment Growth Rates': column_name_1}, inplace=True)
        temp_df = temp_df.reset_index(drop = True)
        filter_df = pd.DataFrame()
        filter_df['Category'] = ""
        for i in range(len(temp_df)):
            eco_indi = temp_df['ECONOMIC INDICATORS'][i]
            for key,value in dict_fetch.items():
                if key == eco_indi:
                    filter_df = filter_df.append(temp_df[temp_df['ECONOMIC INDICATORS'] == eco_indi])
                    filter_df['Category'][i] = value
        filter_df = filter_df[['ECONOMIC INDICATORS', 'Q3-21', 'Q2-21', 'Q3-20', '2020', '2019', 'Category']]
        filter_df = filter_df.reset_index(drop = True)
        filter_df['state_alias'] = (file_path.split('\\')[-1]).split('.')[0]
        return filter_df
    except:
        lf_paths.append(file_path)

file_path = ''
final_df = pd.DataFrame()
for file in download_list:
    file_path = files_path + file
    temp_df = extract(file_path)
    final_df = final_df.append(temp_df)
final_df = final_df.reset_index(drop = True)

print(lf_paths)

def extract_handle_1(failed_path):
    tables = camelot.read_pdf(filepath=failed_path,flavor='stream',edge_tol=1500)
    temp_df = tables[1].df
    remove_from_bracket = lambda text : text.split('(', 1)[0]
    remove_spaces = lambda text : text.strip()
    temp_df[0] = temp_df[0].map(remove_from_bracket).map(remove_spaces)
    # temp_df = temp_df.drop([1], axis=1)
    temp_df = temp_df.replace(r'^\s*$', np.nan, regex=True)
    temp_df = temp_df.dropna()
    temp_df = temp_df.reset_index(drop = True)
    temp_df.columns = temp_df.iloc[0]
    temp_df = temp_df.drop(temp_df.index[0])
    temp_df.rename(columns={'Employment Growth Rates': 'ECONOMIC INDICATORS'}, inplace=True)
    temp_df = temp_df.reset_index(drop = True)
    filter_df = pd.DataFrame()
    filter_df['Category'] = ""
    for i in range(len(temp_df)):
        eco_indi = temp_df['ECONOMIC INDICATORS'][i]
        for key,value in dict_fetch.items():
            if key == eco_indi:
                filter_df = filter_df.append(temp_df[temp_df['ECONOMIC INDICATORS'] == eco_indi])
                filter_df['Category'][i] = value
    filter_df = filter_df[['ECONOMIC INDICATORS', 'Q3-21', 'Q2-21', 'Q3-20', '2020', '2019', 'Category']]
    filter_df = filter_df.reset_index(drop = True)
    filter_df['state_alias'] = (failed_path.split('\\')[-1]).split('.')[0]
    return filter_df
def extract_handle_2(failed_path):
    tables = camelot.read_pdf(filepath=failed_path,flavor='stream',edge_tol=1500)
    temp_df = tables[0].df
    remove_from_bracket = lambda text : text.split('(', 1)[0]
    remove_spaces = lambda text : text.strip()
    temp_df[0] = temp_df[0].map(remove_from_bracket).map(remove_spaces)
    temp_df = temp_df.drop([1], axis=1)
    temp_df = temp_df.replace(r'^\s*$', np.nan, regex=True)
    temp_df = temp_df.dropna()
    temp_df = temp_df.reset_index(drop = True)
    temp_df.columns = temp_df.iloc[0]
    temp_df = temp_df.drop(temp_df.index[0])
    temp_df.rename(columns={'Employment Growth Rates': 'ECONOMIC INDICATORS'}, inplace=True)
    temp_df = temp_df.reset_index(drop = True)
    filter_df = pd.DataFrame()
    filter_df['Category'] = ""
    for i in range(len(temp_df)):
        eco_indi = temp_df['ECONOMIC INDICATORS'][i]
        for key,value in dict_fetch.items():
            if key == eco_indi:
                filter_df = filter_df.append(temp_df[temp_df['ECONOMIC INDICATORS'] == eco_indi])
                filter_df['Category'][i] = value
    filter_df = filter_df[['ECONOMIC INDICATORS', 'Q3-21', 'Q2-21', 'Q3-20', '2020', '2019', 'Category']]
    filter_df = filter_df.reset_index(drop = True)
    filter_df['state_alias'] = (failed_path.split('\\')[-1]).split('.')[0]
    return filter_df
try_success_df = pd.DataFrame()
for failed_path in lf_paths:
    try:
        try_df = extract_handle_1(failed_path)
        print("Success")
    except:
        try_df = extract_handle_2(failed_path)
        print("Success Except")
    try_success_df = try_success_df.append(try_df)
try_success_df = try_success_df.reset_index(drop = True)
absolute_df = pd.DataFrame()
absolute_df = absolute_df.append(final_df)
absolute_df = absolute_df.append(try_success_df)
absolute_df = absolute_df.reset_index(drop = True)
state_lookup = pd.read_csv('D:\\BYTEIQ\\fdic\\fdic\\state_lookup.csv')
all_states = pd.merge(absolute_df, state_lookup, on="state_alias")
all_states = all_states.astype({'Q3-21': float, 'Q2-21': float, 'Q3-20': float, '2020': float, '2019': float})
all_states.to_csv('new_csv.csv')