import os
import pandas as pd
from datetime import datetime

'''
Needed data for overview:
- Disaster name
- Country
- Date
- Operation type
- Operation status
- Operation budget
- Achievements (late, early, not completed) -> for performance
- Active operation count
- Missing columns (to be filled) -> for data completeness
'''

def convert_date(date_str):
    if date_str == "-":
        return pd.NaT
    return datetime.strptime(date_str, "%Y-%m-%d")

def determine_status(row, limit):
    if pd.isna(row[row.columns[1]]) or pd.isna(row[row.columns[1]]):
        return "Not Achieved"
    delta = (row[row.columns[1]] - row[row.columns[0]]).days
    if delta > limit:
        return "Achieved Late"
    return "Achieved Early"

def determine_done(row):
    if pd.isna(row[row.columns[0]]):
        return "Not Achieved"
    return "Achieved"

def process_ea(ea):
    op = ea["operational_progresses"].copy()
    dash = ea["dashboard_progress"].copy()
    fin = ea["financial_progress"].copy()

    num_rows = ea["disasters"].shape[0]
    ea["achievements"] = pd.DataFrame()
    start_date = ea["disasters"]["Start date"].apply(convert_date)
    nfi = ea["nfi"].copy()

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    dn = dash.columns
    for col in dn:
        dash[col] = dash[col].apply(convert_date)
    
    fn = fin.columns
    for col in fn:
        fin[col] = fin[col].apply(convert_date)
    
    nn = nfi.columns
    for col in nn:
        nfi[col] = nfi[col].apply(convert_date)    
    
    ea["achievements"]["Ref"] = ea["disasters"]["Ref"]
    ea["achievements"][on[0]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea["achievements"][on[1]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea["achievements"][on[2]] = pd.merge(op[on[1]], op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea["achievements"][on[3]] = pd.merge(op[on[2]], op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea["achievements"][on[4]] = pd.merge(op[on[3]], op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea["achievements"][on[5]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea["achievements"][on[6]] = pd.merge(op[on[5]], op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(0,), axis=1)
    ea["achievements"][on[7]] = pd.merge(op[on[6]], op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea["achievements"][on[8]] = pd.merge(op[on[2]], op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea["achievements"][on[9]] = pd.merge(op[on[8]], op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea["achievements"][on[10]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea["achievements"][on[12]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea["achievements"][on[13]] = pd.merge(op[on[2]], op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea["achievements"][on[14]] = op[on[14]].apply(determine_done)
    ea["achievements"][on[15]] = op[on[15]].apply(determine_done)
    ea["achievements"][nn[0]] = pd.merge(op[on[2]], op[on[14]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea["achievements"][nn[1]] = pd.merge(op[on[14]], op[on[15]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea["achievements"][dn[0]] = pd.merge(op[on[2]], dash[dn[0]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea["achievements"][dn[1]] = pd.merge(op[on[2]], dash[dn[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea["achievements"][dn[2]] = pd.merge(op[on[2]], dash[dn[2]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea["achievements"][dn[3]] = pd.merge(op[on[2]], dash[dn[3]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea["achievements"][dn[4]] = pd.merge(op[on[2]], dash[dn[4]], left_index=True, right_index=True).apply(determine_status, args=(31,), axis=1)
    ea["achievements"][fn[0]] = pd.merge(op[on[2]], fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea["achievements"][fn[1]] = pd.merge(op[on[2]], fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(10,), axis=1)
    ea["achievements"][fn[2]] = pd.merge(fin[fn[1]], fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea["achievements"][fn[3]] = pd.merge(fin[fn[1]], fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)

    return ea


def process_dref(dref):
    return dref


def process_mcmr(mcmr):
    return mcmr


def process_pcce(pcce):
    return pcce



def merge_dfs(folder):
    files = os.listdir(folder)
    files = [f for f in files if f.endswith('.csv')]
    bucket = {}
    for file in files:
        key = file.split(".")[0]
        value = pd.read_csv(folder + "/" + file, index_col="Ref")
        bucket[key] = value
    
    return bucket

def generate_overview():
    ea = process_ea(merge_dfs("../organized_ea"))
    dref = merge_dfs("../organized_dref")
    mcmr = merge_dfs("../organized_mcmr")
    pcce = merge_dfs("../organized_pcce")


