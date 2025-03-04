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

# missing data if it the date is in the future
# uncomplete IF it the due date has passed
# Missing, Not Achieved, Achieved Early, Achieved Late, Achieved

def full_list(cols):
    output = []
    if not isinstance(cols, list):
        cols = [cols]
    for col in cols:
        output.append(col)
        output.append(f"{col} (days)")
    return output


def summarize_df(df):
    df = df.copy()
    categories = ["Achieved", "Not Achieved", "Achieved Early", "Achieved Late", "TBD", "DNU", "Missing"]

    for category in categories:
        df.loc[:, category] = df.apply(lambda x: sum(str(cell) == category for cell in x), axis=1)
    
    df.loc[:, "Data Completeness"] = (df["Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Not Achieved"]) / \
                                    (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Missing"])
    df.loc[:, "General Performance"] = (((df["Achieved"] + df["Achieved Early"]) * 2) + df["Achieved Late"]) / \
                                    ((df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 2)

    cols_to_move = ["Achieved", "Not Achieved", "Missing", "Achieved Early", "Achieved Late", "TBD", "DNU", "Data Completeness", "General Performance"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]

    return df


def update_general_info(folder):
    files = os.listdir(folder)
    df = pd.read_csv(f"{folder}general_info.csv", index_col="Ref")

    for file in files:
        if file == "general_info.csv":
            continue
        temp = pd.read_csv(f"{folder}{file}", index_col="Ref")
        df["Achieved"] = temp["Achieved"] if "Achieved" not in df.columns else df["Achieved"] + temp["Achieved"]
        df["Not Achieved"] = temp["Not Achieved"] if "Not Achieved" not in df.columns else df["Not Achieved"] + temp["Not Achieved"]
        df["Missing"] = temp["Missing"] if "Missing" not in df.columns else df["Missing"] + temp["Missing"]
        df["Achieved Early"] = temp["Achieved Early"] if "Achieved Early" not in df.columns else df["Achieved Early"] + temp["Achieved Early"]
        df["Achieved Late"] = temp["Achieved Late"] if "Achieved Late" not in df.columns else df["Achieved Late"] + temp["Achieved Late"]
        df["TBD"] = temp["TBD"] if "TBD" not in df.columns else df["TBD"] + temp["TBD"]
        df["DNU"] = temp["DNU"] if "DNU" not in df.columns else df["DNU"] + temp["DNU"]
    
    df.loc[:, "Data Completeness"] = (df["Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Not Achieved"]) / \
                                    (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Missing"])
    df.loc[:, "General Performance"] = (((df["Achieved"] + df["Achieved Early"]) * 2) + df["Achieved Late"]) / \
                                    ((df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 2)

    df.to_csv(f"{folder}general_info.csv", index=True)


def area_split_ea(overview, columns):
    msr_column = "MSR ready (compliant or resource allocated)"
    folder = "../organized_ea/"
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:15] + [columns[27]])] # add EA coverage
    surge = overview[full_list(columns[28:31])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[44:46])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[46:50])] # missing joint statement in master data
    logistics = overview[full_list(columns[50:53])]
    im = overview[columns[53:58]]
    finance = overview[full_list(columns[58:62])]
    security = overview[[msr_column, f"{msr_column} (days)"]]

    summarize_df(assessment).to_csv(f"{folder}assessment.csv", index=True)
    summarize_df(resource_mobilization).to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    summarize_df(surge).to_csv(f"{folder}surge.csv", index=True)
    summarize_df(hr).to_csv(f"{folder}hr_planning_and_recruitement.csv", index=True)
    summarize_df(coordination).to_csv(f"{folder}coordination.csv", index=True)
    summarize_df(logistics).to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    summarize_df(im).to_csv(f"{folder}information_management.csv", index=True)
    summarize_df(finance).to_csv(f"{folder}financial_management.csv", index=True)
    summarize_df(security).to_csv(f"{folder}security.csv", index=True)
    update_general_info(folder)

def area_split_dref(overview, columns):
    folder = "../organized_dref/"
    msr_column = "MSR ready (compliant or resource allocated)"

    assessment = overview[full_list(columns[11])]
    risk = overview[full_list(columns[12:14])]
    resource_mobilization = overview[full_list(columns[14:16])]
    surge = overview[full_list(columns[16:19])]
    logistics = overview[full_list(columns[32:34])]
    finance = overview[full_list(columns[34:38])]
    delivery = overview[full_list([columns[38]])] # add targeted population, ehi distribution, and implementation rate
    security = overview[[msr_column, f"{msr_column} (days)"]]

    summarize_df(assessment).to_csv(f"{folder}assessment.csv", index=True)
    summarize_df(risk).to_csv(f"{folder}risk_and_accountability.csv", index=True)
    summarize_df(resource_mobilization).to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    summarize_df(surge).to_csv(f"{folder}surge.csv", index=True)
    summarize_df(logistics).to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    summarize_df(finance).to_csv(f"{folder}financial_management.csv", index=True)
    summarize_df(delivery).to_csv(f"{folder}programme_delivery.csv", index=True)
    summarize_df(security).to_csv(f"{folder}security.csv", index=True)
    update_general_info(folder)

def area_split_mcmr(overview, columns):
    folder = "../organized_mcmr/"
    resource_mobilization = overview[full_list(columns[11:14] + [columns[38]])] # add coverage
    surge = overview[full_list(columns[20:22])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[35:37])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[37])]
    logistics = overview[full_list(columns[38:41])]
    im = overview[columns[41:42]]
    finance = overview[full_list(columns[42:44])]

    summarize_df(resource_mobilization).to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    summarize_df(surge).to_csv(f"{folder}surge.csv", index=True)
    summarize_df(hr).to_csv(f"{folder}hr_planning_and_recruitement.csv", index=True)
    summarize_df(coordination).to_csv(f"{folder}coordination.csv", index=True)
    summarize_df(logistics).to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    summarize_df(im).to_csv(f"{folder}information_management.csv", index=True)
    summarize_df(finance).to_csv(f"{folder}financial_management.csv", index=True)
    update_general_info(folder)

def area_split_pcce(overview, columns):
    folder = "../organized_pcce/"
    msr_column = "MSR ready (compliant or resource allocated)"

    assessment = overview[columns[11:12]]
    resource_mobilization = overview[full_list(columns[12:15] + [columns[21]])]
    surge = overview[full_list(columns[22:25])]
    hr = overview[full_list(columns[39:41])]
    coordination = overview[full_list(columns[41:44])]
    logistics = overview[full_list(columns[44:47])]
    im = overview[columns[47:52]]
    finance = overview[full_list(columns[52:56])]
    # delivery = overview[full_list(columns[55:57])] # add percentage of targeted population receiving assistance and % of planned budget implementation
    security = overview[[msr_column, f"{msr_column} (days)"]]

    summarize_df(assessment).to_csv(f"{folder}assessment.csv", index=True)
    summarize_df(resource_mobilization).to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    summarize_df(surge).to_csv(f"{folder}surge.csv", index=True)
    summarize_df(hr).to_csv(f"{folder}hr_planning_and_recruitement.csv", index=True)
    summarize_df(coordination).to_csv(f"{folder}coordination.csv", index=True)
    summarize_df(logistics).to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    summarize_df(im).to_csv(f"{folder}information_management.csv", index=True)
    summarize_df(finance).to_csv(f"{folder}financial_management.csv", index=True)
    # delivery.to_csv(f"{folder}programme_delivery.csv", index=True)
    summarize_df(security).to_csv(f"{folder}security.csv", index=True)
    update_general_info(folder)

def convert_date(date_str):
    if date_str == "-" or pd.isna(date_str):
        return "-"
    if date_str == "DNU":
        return "DNU"
    if date_str == "NA":
        return "NA"
    return datetime.strptime(str(date_str)[:10], "%Y-%m-%d")

def determine_status(row, limit):
    keys = row.index.tolist()
    r0, r1 = row.iloc[0], row.iloc[1]

    if r1 == "-":    
        deadline = r0 + pd.Timedelta(days=limit)
        if deadline > datetime.now():
            return pd.Series(["TBD", -1], index=[keys[1], f"{keys[1]} (days)"]) 
        return pd.Series(["Missing", -1], index=[keys[1], f"{keys[1]} (days)"])
    
    if r1 == "DNU":
        return pd.Series(["DNU", -1], index=[keys[1], f"{keys[1]} (days)"])
    
    if r1 == "NA":
        return pd.Series(["NA", -1], index=[keys[1], f"{keys[1]} (days)"])
    
    days = (r1 - r0).days
    if days > limit:
        return pd.Series(["Achieved Late", days], index=[keys[1], f"{keys[1]} (days)"])

    return pd.Series(["Achieved Early", days], index=[keys[1], f"{keys[1]} (days)"])


def determine_done(row):
    if row == "-":
        return "Missing"
    if row == "DNU":
        return "DNU"
    if row == "NA":
        return "Not Achieved"
    return "Achieved"

def msr_ready(row ,limit):
    msr_column = "MSR ready (compliant or resource allocated)"
    r0, r1, r2 = row.iloc[0], row.iloc[1], row.iloc[2]
    deadline = r0 + pd.Timedelta(days=limit)

    if (pd.isna(r1) or r1 == "-") and (pd.isna(r2) or r2 == "-"):        
        if deadline > datetime.now():
            return pd.Series(["TBD", -1], index=[msr_column, f"{msr_column} (days)"])
        return pd.Series(["Missing", -1], index=[msr_column, f"{msr_column} (days)"])
    elif pd.isna(r1) or r1 == "-":
        days = (r2 - r0).days
        return pd.Series(["Achieved Late" if days > limit else "Achieved Early", days], index=[msr_column, f"{msr_column} (days)"])
    else:
        days = (r1 - r0).days
        return pd.Series(["Achieved Late" if days > limit else "Achieved Early", days], index=[msr_column, f"{msr_column} (days)"])

def process_ea(ea):
    op = ea["operational_progresses"].copy()
    dash = ea["dashboard_progress"].copy()
    fin = ea["financial_progress"].copy()
    nfi = ea["nfi"].copy()
    key = "achievements"

    msr_column = "MSR ready (compliant or resource allocated)"

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
    ea[key][[on[2], f"{on[2]} (days)"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    ea[key][[on[3], f"{on[3]} (days)"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[on[4], f"{on[4]} (days)"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(18,), axis=1)
    ea[key][[on[5], f"{on[5]} (days)"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[on[6], f"{on[6]} (days)"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[7], f"{on[7]} (days)"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[on[8], f"{on[8]} (days)"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[on[9], f"{on[9]} (days)"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(13,), axis=1)
    ea[key][[on[10], f"{on[10]} (days)"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[11], f"{on[11]} (days)"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    ea[key][[on[12], f"{on[12]} (days)"]] = pd.merge(start_date, op[on[11]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[on[13], f"{on[13]} (days)"]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[14], f"{on[14]} (days)"]] = pd.merge(start_date, op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[msr_column, f"{msr_column} (days)"]] = pd.merge(start_date, op[[on[15], on[16]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    ea[key][[nn[0], f"{nn[0]} (days)"]] = pd.merge(start_date, nfi[nn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[nn[1], f"{nn[1]} (days)"]] = pd.merge(start_date, nfi[nn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[nn[2], f"{nn[2]} (days)"]] = pd.merge(start_date, nfi[nn[2]], left_index=True, right_index=True).apply(determine_status, args=(28,), axis=1)
    ea[key][dn[0]] = dash[dn[0]].apply(determine_done)
    ea[key][dn[1]] = dash[dn[1]].apply(determine_done)
    ea[key][dn[2]] = dash[dn[2]].apply(determine_done)
    ea[key][dn[3]] = dash[dn[3]].apply(determine_done)
    ea[key][dn[4]] = dash[dn[4]].apply(determine_done)
    ea[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[fn[2], f"{fn[2]} (days)"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(16,), axis=1)
    ea[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(20,), axis=1)
    ea[key][[fn[4], f"{fn[4]} (days)"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)

    return ea

def process_dref(dref):
    op = dref["operational_progresses"].copy()
    fin = dref["financial_progress"].copy()
    key = "achievements"
    msr_column = "MSR ready (compliant or resource allocated)"

    dref[key] = pd.DataFrame()
    start_date = dref["disasters"]["Trigger Date "].apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    fn = fin.columns
    for col in fn[:5]:
        fin[col] = fin[col].apply(convert_date)
    
    dref[key]["Ref"] = dref["disasters"].index
    dref[key].set_index("Ref", inplace=True)

    dref[key][[on[0], f"{on[0]} (days)"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    dref[key][[on[1], f"{on[1]} (days)"]] = pd.merge(op[on[4]], op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    dref[key][[on[2], f"{on[2]} (days)"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    dref[key][[on[3], f"{on[3]} (days)"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1) # check for viability (10 if NS started moving)
    dref[key][[on[4], f"{on[4]} (days)"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    dref[key][[on[5], f"{on[5]} (days)"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    dref[key][[on[6], f"{on[6]} (days)"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    dref[key][[on[7], f"{on[7]} (days)"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    dref[key][[on[8], f"{on[8]} (days)"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    dref[key][[on[9], f"{on[9]} (days)"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    dref[key][[msr_column, f"{msr_column} (days)"]] = pd.merge(start_date, op[[on[10], on[11]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    
    dref[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    dref[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    dref[key][[fn[2], f"{fn[2]} (days)"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(16,), axis=1)
    dref[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(20,), axis=1)
    dref[key][[fn[4], f"{fn[4]} (days)"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)

    return dref

def process_mcmr(mcmr):
    op = mcmr["operational_progresses"].copy()
    fin = mcmr["financial_progress"].copy()
    key = "achievements"

    mcmr[key] = pd.DataFrame()
    start_date = mcmr["disasters"][mcmr["disasters"].columns[1]].apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    fn = fin.columns
    for col in fn:
        fin[col] = fin[col].apply(convert_date)
    
    mcmr[key]["Ref"] = mcmr["disasters"].index
    mcmr[key].set_index("Ref", inplace=True)

    mcmr[key][[on[0], f"{on[0]} (days)"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    mcmr[key][[on[1], f"{on[1]} (days)"]] = pd.merge(op[on[0]], op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    mcmr[key][[on[2], f"{on[2]} (days)"]] = pd.merge(op[on[1]], op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    mcmr[key][[on[3], f"{on[3]} (days)"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1) # check for viability (10 if NS started moving)
    mcmr[key][[on[4], f"{on[4]} (days)"]] = pd.merge(op[on[3]], op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    mcmr[key][[on[5], f"{on[5]} (days)"]] = pd.merge(op[on[1]], op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    mcmr[key][[on[6], f"{on[6]} (days)"]] = pd.merge(op[on[5]], op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    mcmr[key][[on[7], f"{on[7]} (days)"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    mcmr[key][[on[8], f"{on[8]} (days)"]] = pd.merge(op[on[1]], op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    mcmr[key][[on[9], f"{on[9]} (days)"]] = pd.merge(op[on[1]], op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    mcmr[key][[on[10], f"{on[10]} (days)"]] = pd.merge(op[on[9]], op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    mcmr[key][on[11]] = op[on[11]].apply(determine_done)
    mcmr[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(op[on[1]], fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(5,), axis=1)
    mcmr[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(fin[fn[0]], fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(5,), axis=1)

    return mcmr
    

def process_pcce(pcce):
    op = pcce["operational_progresses"].copy()
    dash = pcce["dashboard"].copy()
    fin = pcce["financial_progress"].copy()
    key = "achievements"
    msr_column = "MSR ready (compliant or resource allocated)"


    pcce[key] = pd.DataFrame()
    start_date = pcce["disasters"][pcce["disasters"].columns[1]].apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    dn = dash.columns
    for col in dn:
        dash[col] = dash[col].apply(convert_date)
    
    fn = fin.columns
    for col in fn:
        fin[col] = fin[col].apply(convert_date)    
    
    pcce[key]["Ref"] = pcce["disasters"].index
    pcce[key].set_index("Ref", inplace=True)
    pcce[key][on[0]] = op[on[0]].apply(determine_done)
    pcce[key][[on[1], f"{on[1]} (days)"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[on[2], f"{on[2]} (days)"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    pcce[key][[on[3], f"{on[3]} (days)"]] = pd.merge(op[on[2]], op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    pcce[key][[on[4], f"{on[4]} (days)"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)
    pcce[key][[on[5], f"{on[5]} (days)"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(5,), axis=1)
    pcce[key][[on[6], f"{on[6]} (days)"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    pcce[key][[on[7], f"{on[7]} (days)"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(6,), axis=1)
    pcce[key][[on[8], f"{on[8]} (days)"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[9], f"{on[9]} (days)"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(13,), axis=1)
    pcce[key][[on[10], f"{on[10]} (days)"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    pcce[key][[on[11], f"{on[11]} (days)"]] = pd.merge(start_date, op[on[11]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[on[12], f"{on[12]} (days)"]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    pcce[key][[on[13], f"{on[13]} (days)"]] = pd.merge(op[on[2]], op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[14], f"{on[14]} (days)"]] = pd.merge(op[on[2]], op[on[14]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[15], f"{on[15]} (days)"]] = pd.merge(start_date, op[on[15]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    pcce[key][[on[16], f"{on[16]} (days)"]] = pd.merge(op[on[2]], op[on[16]], left_index=True, right_index=True).apply(determine_status, args=(28,), axis=1)
    pcce[key][[msr_column, f"{msr_column} (days)"]] = pd.merge(start_date, op[[on[17], on[18]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    pcce[key][dn[0]] = dash[dn[0]].apply(determine_done)
    pcce[key][dn[1]] = dash[dn[1]].apply(determine_done)
    pcce[key][dn[2]] = dash[dn[2]].apply(determine_done)
    pcce[key][dn[3]] = dash[dn[3]].apply(determine_done)
    pcce[key][dn[4]] = dash[dn[4]].apply(determine_done)
    pcce[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    pcce[key][[fn[2], f"{fn[2]} (days)"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(16,), axis=1)
    pcce[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(20,), axis=1)
    pcce[key][[fn[4], f"{fn[4]} (days)"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)

    return pcce


def generate_overview(bucket, sheets):
    ea = process_ea(bucket["EA"])
    dref = process_dref(bucket["DREF"])
    mcmr = process_mcmr(bucket["MCMR"])
    pcce = process_pcce(bucket["Protracted"])

    key = "achievements"
    for sheet_name, sheet in sheets.items():
        if "EA" in sheet_name:
            area_split_ea(ea[key], sheet.columns.tolist())
        elif "DREF" in sheet_name:
            area_split_dref(dref[key], sheet.columns.tolist())
        elif "MCMR" in sheet_name:
            area_split_mcmr(mcmr[key], sheet.columns.tolist())
        elif "Protracted" in sheet_name:
            area_split_pcce(pcce[key], sheet.columns.tolist())
        else:
            continue
    print("All complete")
    
