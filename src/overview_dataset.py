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
    if date_str == "-" or pd.isna(date_str):
        return pd.NaT
    return datetime.strptime(str(date_str)[:10], "%Y-%m-%d")

def determine_status(row, limit):
    keys = row.index.tolist()
    if pd.isna(row[keys[0]]) or pd.isna(row[keys[1]]):
        return pd.Series(["Not Achieved", -1], index=[keys[1], f"{keys[1]} (days)"])
    days = (row[keys[1]] - row[keys[0]]).days
    if days > limit:
        return pd.Series(["Achieved Late", days], index=[keys[1], f"{keys[1]} (days)"])

    return pd.Series(["Achieved Early", days], index=[keys[1], f"{keys[1]} (days)"])


def determine_done(row):
    if pd.isna(row) or row == "-":
        return "Not Achieved"
    return "Achieved"

def process_ea(ea):
    op = ea["operational_progresses"].copy()
    dash = ea["dashboard_progress"].copy()
    fin = ea["financial_progress"].copy()
    nfi = ea["nfi"].copy()
    key = "achievements"

    ea[key] = pd.DataFrame()
    start_date = ea["disasters"]["Trigger Date "].apply(convert_date)

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
    for col in nn[:3]:
        nfi[col] = nfi[col].apply(convert_date)    
    
    ea[key]["Ref"] = ea["disasters"].index
    ea[key].set_index("Ref", inplace=True)
    ea[key][[on[0], f"{on[0]} (days)"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[on[1], f"{on[1]} (days)"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[on[2], f"{on[2]} (days)"]] = pd.merge(op[on[1]], op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[on[3], f"{on[3]} (days)"]] = pd.merge(op[on[2]], op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[on[4], f"{on[4]} (days)"]] = pd.merge(op[on[3]], op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[on[5], f"{on[5]} (days)"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[on[6], f"{on[6]} (days)"]] = pd.merge(op[on[5]], op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(0,), axis=1)
    ea[key][[on[7], f"{on[7]} (days)"]] = pd.merge(op[on[6]], op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[8], f"{on[8]} (days)"]] = pd.merge(op[on[2]], op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[on[9], f"{on[9]} (days)"]] = pd.merge(op[on[8]], op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[10], f"{on[10]} (days)"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][on[11]] = op[on[11]].apply(determine_done)
    ea[key][[on[12], f"{on[12]} (days)"]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[on[13], f"{on[13]} (days)"]] = pd.merge(op[on[2]], op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][on[14]] = op[on[14]].apply(determine_done)
    ea[key][on[15]] = op[on[15]].apply(determine_done)
    ea[key][[nn[0], f"{nn[0]} (days)"]] = pd.merge(op[on[2]], nfi[nn[0]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[nn[1], f"{nn[1]} (days)"]] = pd.merge(nfi[nn[0]], nfi[nn[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[dn[0], f"{dn[0]} (days)"]] = pd.merge(op[on[2]], dash[dn[0]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[dn[1], f"{dn[1]} (days)"]] = pd.merge(op[on[2]], dash[dn[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[dn[2], f"{dn[2]} (days)"]] = pd.merge(op[on[2]], dash[dn[2]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[dn[3], f"{dn[3]} (days)"]] = pd.merge(op[on[2]], dash[dn[3]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[dn[4], f"{dn[4]} (days)"]] = pd.merge(op[on[2]], dash[dn[4]], left_index=True, right_index=True).apply(determine_status, args=(31,), axis=1)
    ea[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(op[on[2]], fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(op[on[2]], fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(10,), axis=1)
    ea[key][[fn[2], f"{fn[2]} (days)"]] = pd.merge(fin[fn[1]], fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(fin[fn[1]], fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key]["Achieved"] = ea[key].apply(lambda x: x.str.startswith("Achieved").sum(), axis=1)
    ea[key]["Not Achieved"] = ea[key].apply(lambda x: x.str.contains("Not Achieved").sum(), axis=1)
    ea[key]["Achieved Early"] = ea[key].apply(lambda x: x.str.contains("Achieved Early").sum(), axis=1)
    ea[key]["Achieved Late"] = ea[key].apply(lambda x: x.str.contains("Achieved Late").sum(), axis=1)
    ea[key] = ea[key][ea[key].columns[-4:].tolist() + ea[key].columns[:-4].tolist()]
    ea[key].to_csv("../organized_ea/ea_overview.csv", index=True)
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
    # dref = merge_dfs("../organized_dref")
    # mcmr = merge_dfs("../organized_mcmr")
    # pcce = merge_dfs("../organized_pcce")


generate_overview()