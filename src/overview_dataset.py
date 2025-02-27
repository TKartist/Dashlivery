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


def area_split_ea(overview, columns):
    folder = "../organized_ea/"
    assessment = overview[full_list(columns[10])]
    resource_mobilization = overview[full_list(columns[11:14] + [columns[26]])] # add EA coverage
    surge = overview[full_list(columns[27:30])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[43:45])] # add % related values to the hr (rrp)
    coordination = overview[columns[45:48]] # missing joint statement in master data
    logistics = overview[columns[48:51]]
    im = overview[columns[51:56]]
    finance = overview[columns[56:60]]
    security = overview[columns[73:75]]

    assessment.to_csv(f"{folder}assessment.csv", index=True)
    resource_mobilization.to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    surge.to_csv(f"{folder}surge.csv", index=True)
    hr.to_csv(f"{folder}hr_planning_and_recruitement.csv", index=True)
    coordination.to_csv(f"{folder}coordination.csv", index=True)
    logistics.to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    im.to_csv(f"{folder}information_management.csv", index=True)
    finance.to_csv(f"{folder}financial_management.csv", index=True)
    security.to_csv(f"{folder}security.csv", index=True)

def area_split_dref(overview, columns):
    folder = "../organized_dref/"
    assessment = overview[full_list(columns[10])]
    risk = overview[full_list(columns[11:13])]
    resource_mobilization = overview[full_list(columns[13:15])]
    surge = overview[full_list(columns[15:18])]
    logistics = overview[full_list(columns[31:33])]
    finance = overview[full_list(columns[33:37])]
    delivery = overview[full_list([columns[37]])] # add targeted population, ehi distribution, and implementation rate
    security = overview[columns[50:52]]

    assessment.to_csv(f"{folder}assessment.csv", index=True)
    risk.to_csv(f"{folder}risk_and_accountability.csv", index=True)
    resource_mobilization.to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    surge.to_csv(f"{folder}surge.csv", index=True)
    logistics.to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    finance.to_csv(f"{folder}financial_management.csv", index=True)
    delivery.to_csv(f"{folder}programme_delivery.csv", index=True)
    security.to_csv(f"{folder}security.csv", index=True)

def area_split_mcmr(overview, columns):
    folder = "../organized_mcmr/"
    resource_mobilization = overview[full_list(columns[10:13] + [columns[37]])] # add coverage
    surge = overview[full_list(columns[19:21])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[34:36])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[36])]
    logistics = overview[full_list(columns[37:40])]
    im = overview[columns[40]]
    finance = overview[full_list(columns[41:43])]

    resource_mobilization.to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    surge.to_csv(f"{folder}surge.csv", index=True)
    hr.to_csv(f"{folder}hr_planning_and_recruitement.csv", index=True)
    coordination.to_csv(f"{folder}coordination.csv", index=True)
    logistics.to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    im.to_csv(f"{folder}information_management.csv", index=True)
    finance.to_csv(f"{folder}financial_management.csv", index=True)

def area_split_pcce(overview, columns):
    folder = "../organized_pcce/"
    assessment = overview[full_list(columns[10])]
    resource_mobilization = overview[full_list(columns[11:14] + [columns[20]])]
    surge = overview[full_list(columns[21:24])]
    hr = overview[full_list(columns[38:40])]
    coordination = overview[full_list(columns[40:43])]
    logistics = overview[full_list(columns[43:46])]
    im = overview[columns[46:51]]
    finance = overview[full_list(columns[51:55])]
    # delivery = overview[full_list(columns[55:57])] # add percentage of targeted population receiving assistance and % of planned budget implementation
    security = overview[columns[59:61]]
    print(assessment)

    assessment.to_csv(f"{folder}assessment.csv", index=True)
    resource_mobilization.to_csv(f"{folder}planning_and_resource_mobilization.csv", index=True)
    surge.to_csv(f"{folder}surge.csv", index=True)
    hr.to_csv(f"{folder}hr_planning_and_recruitement.csv", index=True)
    coordination.to_csv(f"{folder}coordination.csv", index=True)
    logistics.to_csv(f"{folder}procurement_and_logistics.csv", index=True)
    im.to_csv(f"{folder}information_management.csv", index=True)
    finance.to_csv(f"{folder}financial_management.csv", index=True)
    # delivery.to_csv(f"{folder}programme_delivery.csv", index=True)
    security.to_csv(f"{folder}security.csv", index=True)

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
    ea[key][dn[0]] = dash[dn[0]].apply(determine_done)
    ea[key][dn[1]] = dash[dn[1]].apply(determine_done)
    ea[key][dn[2]] = dash[dn[2]].apply(determine_done)
    ea[key][dn[3]] = dash[dn[3]].apply(determine_done)
    ea[key][dn[4]] = dash[dn[4]].apply(determine_done)
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
    op = dref["operational_progresses"].copy()
    fin = dref["financial_progress"].copy()
    key = "achievements"

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
    dref[key][[on[6], f"{on[6]} (days)"]] = pd.merge(op[on[5]], op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    dref[key][[on[7], f"{on[7]} (days)"]] = pd.merge(op[on[6]], op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    dref[key][[on[8], f"{on[8]} (days)"]] = pd.merge(op[on[4]], op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    dref[key][[on[9], f"{on[9]} (days)"]] = pd.merge(op[on[8]], op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    dref[key][on[10]] = op[on[10]].apply(determine_done)
    dref[key][on[11]] = op[on[11]].apply(determine_done)
    dref[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(op[on[4]], fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    dref[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(fin[fn[0]], fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(10,), axis=1)
    dref[key][[fn[2], f"{fn[2]} (days)"]] = pd.merge(fin[fn[1]], fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    dref[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(fin[fn[2]], fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    dref[key][[fn[4], f"{fn[4]} (days)"]] = pd.merge(op[on[4]], fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(30,), axis=1)
    dref[key]["Achieved"] = dref[key].apply(lambda x: x.str.startswith("Achieved").sum(), axis=1)
    dref[key]["Not Achieved"] = dref[key].apply(lambda x: x.str.contains("Not Achieved").sum(), axis=1)
    dref[key]["Achieved Early"] = dref[key].apply(lambda x: x.str.contains("Achieved Early").sum(), axis=1)
    dref[key]["Achieved Late"] = dref[key].apply(lambda x: x.str.contains("Achieved Late").sum(), axis=1)
    dref[key] = dref[key][dref[key].columns[-4:].tolist() + dref[key].columns[:-4].tolist()]
    dref[key].to_csv("../organized_dref/dref_overview.csv", index=True)
    

    return dref

def process_mcmr(mcmr):
    op = mcmr["operational_progresses"].copy()
    fin = mcmr["financial_progress"].copy()
    key = "achievements"

    mcmr[key] = pd.DataFrame()
    start_date = mcmr["disasters"][mcmr["disasters"].columns[0]].apply(convert_date)

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
    mcmr[key]["Achieved"] = mcmr[key].apply(lambda x: x.str.startswith("Achieved").sum(), axis=1)
    mcmr[key]["Not Achieved"] = mcmr[key].apply(lambda x: x.str.contains("Not Achieved").sum(), axis=1)
    mcmr[key]["Achieved Early"] = mcmr[key].apply(lambda x: x.str.contains("Achieved Early").sum(), axis=1)
    mcmr[key]["Achieved Late"] = mcmr[key].apply(lambda x: x.str.contains("Achieved Late").sum(), axis=1)
    mcmr[key] = mcmr[key][mcmr[key].columns[-4:].tolist() + mcmr[key].columns[:-4].tolist()]
    mcmr[key].to_csv("../organized_mcmr/mcmr_overview.csv", index=True)

    return mcmr
    

def process_pcce(pcce):
    op = pcce["operational_progresses"].copy()
    dash = pcce["dashboard"].copy()
    fin = pcce["financial_progress"].copy()
    key = "achievements"

    pcce[key] = pd.DataFrame()
    start_date = pcce["disasters"][pcce["disasters"].columns[0]].apply(convert_date)

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
    pcce[key][[on[0], f"{on[0]} (days)"]] = op[on[0]].apply(determine_done)
    pcce[key][[on[1], f"{on[1]} (days)"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[on[2], f"{on[2]} (days)"]] = pd.merge(op[on[1]], op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    pcce[key][[on[3], f"{on[3]} (days)"]] = pd.merge(op[on[2]], op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    pcce[key][[on[4], f"{on[4]} (days)"]] = pd.merge(op[on[3]], op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(28,), axis=1)
    pcce[key][[on[5], f"{on[5]} (days)"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(5,), axis=1)
    pcce[key][[on[6], f"{on[6]} (days)"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    pcce[key][[on[7], f"{on[7]} (days)"]] = pd.merge(op[on[6]], op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(5,), axis=1)
    pcce[key][[on[8], f"{on[8]} (days)"]] = pd.merge(op[on[2]], op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[on[9], f"{on[9]} (days)"]] = pd.merge(op[on[8]], op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    pcce[key][[on[10], f"{on[10]} (days)"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    pcce[key][on[11]] = op[on[11]].apply(determine_done)
    pcce[key][[on[12], f"{on[12]} (days)"]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    pcce[key][[on[13], f"{on[13]} (days)"]] = pd.merge(op[on[2]], op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[on[14], f"{on[14]} (days)"]] = pd.merge(op[on[2]], op[on[14]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[on[15], f"{on[15]} (days)"]] = pd.merge(op[on[14]], op[on[15]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[on[16], f"{on[16]} (days)"]] = pd.merge(op[on[2]], op[on[16]], left_index=True, right_index=True).apply(determine_status, args=(30,), axis=1)
    pcce[key][on[17]] = op[on[17]].apply(determine_done)
    pcce[key][on[18]] = op[on[18]].apply(determine_done)
    pcce[key][dn[0]] = dash[dn[0]].apply(determine_done)
    pcce[key][dn[1]] = dash[dn[1]].apply(determine_done)
    pcce[key][dn[2]] = dash[dn[2]].apply(determine_done)
    pcce[key][dn[3]] = dash[dn[3]].apply(determine_done)
    pcce[key][dn[4]] = dash[dn[4]].apply(determine_done)
    pcce[key][[fn[0], f"{fn[0]} (days)"]] = pd.merge(op[on[2]], fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[fn[1], f"{fn[1]} (days)"]] = pd.merge(op[on[2]], fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(10,), axis=1)
    pcce[key][[fn[2], f"{fn[2]} (days)"]] = pd.merge(fin[fn[1]], fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    pcce[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(fin[fn[1]], fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[fn[3], f"{fn[3]} (days)"]] = pd.merge(op[on[2]], fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key]["Achieved"] = pcce[key].apply(lambda x: x.str.startswith("Achieved").sum(), axis=1)
    pcce[key]["Not Achieved"] = pcce[key].apply(lambda x: x.str.contains("Not Achieved").sum(), axis=1)
    pcce[key]["Achieved pccerly"] = pcce[key].apply(lambda x: x.str.contains("Achieved pccerly").sum(), axis=1)
    pcce[key]["Achieved Late"] = pcce[key].apply(lambda x: x.str.contains("Achieved Late").sum(), axis=1)
    pcce[key] = pcce[key][pcce[key].columns[-4:].tolist() + pcce[key].columns[:-4].tolist()]
    pcce[key].to_csv("../organized_pcce/pcce_overview.csv", index=True)
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

def generate_overview(sheets):
    ea = process_ea(merge_dfs("../organized_ea"))
    dref = process_dref(merge_dfs("../organized_dref"))
    mcmr = process_mcmr(merge_dfs("../organized_mcmr"))
    pcce = process_pcce(merge_dfs("../organized_pcce"))

    key = "achievements"
    for sheet_name, sheet in sheets.items():
        if "EA" in sheet_name:
            area_split_ea(ea[key], sheet.columns.tolist())
        elif "DREF" in sheet_name:
            area_split_dref(dref[key], sheet.columns.tolist())
        elif "MCMR" in sheet_name:
            area_split_mcmr(mcmr[key], sheet.columns.tolist())
        elif "PCCE" in sheet_name:
            area_split_pcce(pcce[key], sheet.columns.tolist())
        else:
            continue
    print("All complete")
    
