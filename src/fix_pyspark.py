from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import to_date
import warnings
import numpy as np
warnings.filterwarnings("ignore")


def organize_ea(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "EA" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:16] + col_name[22:26] + col_name[38:45] + col_name[69:]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[52:57]]
    dashboard_progress = sheet.loc[:, [col_name[0]] + col_name[47:52]]
    sec_coverage = sheet.loc[:, [col_name[0]] + col_name[15:21]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[26:39]]
    nfi = sheet.loc[:, [col_name[0]] + col_name[45:47] + col_name[57:60]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[60:69]]
    
    sec_coverage = sec_coverage.replace('\n', ' ', regex=True)
    sec_coverage = sec_coverage.replace(' ', ' ', regex=True)

    print("EA master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref"), "financial_progress" : financial_progress.set_index("Ref"), "dashboard_progress" : dashboard_progress.set_index("Ref"), "sec_coverage" : sec_coverage.set_index("Ref"), "rrp" : rrp.set_index("Ref"), "nfi" : nfi.set_index("Ref"), "operational_achievements" : operational_achievements.set_index("Ref")}

def organize_dref(sheet):
    col_name = sheet.columns.tolist()
    for col in col_name:
        sheet = sheet.rename(columns={col:col.replace(" ", "_")})
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:19] + col_name[32:34] + col_name[51:53]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[19:32]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[34:41]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[41:51]]

    print("DREF master data organized")
    return {"disasters": disasters.set_index("Ref"), "operational_progresses": operational_progresses.set_index("Ref"), "rrp": rrp.set_index("Ref"), "financial_progress": financial_progress.set_index("Ref"), "operational_achievements": operational_achievements.set_index("Ref")}

def organize_mcmr(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "MCMR" + sheet[col_name[6]] 
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:14] + col_name[20:22] + col_name[35:42]]
    total_coverage = sheet.loc[:, [col_name[0]] + col_name[14:20]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[22:35]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[42:44]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[44:]]

    total_coverage = total_coverage.replace('\n', ' ', regex=True)
    total_coverage = total_coverage.replace(' ', ' ', regex=True)
    print("MCMR master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref"), "total_coverage" : total_coverage.set_index("Ref"), "rrp" : rrp.set_index("Ref"), "financial_progress" : financial_progress.set_index("Ref"), "operational_achievements" : operational_achievements.set_index("Ref")}


def organize_protracted(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "PCCE" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:15] + col_name[21:25] + col_name[39:47] + col_name[57:58] + col_name[60:62]]
    coverage = sheet.loc[:, [col_name[0]] + col_name[15:21]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[25:39]]
    dashboard = sheet.loc[:, [col_name[0]] + col_name[47:52]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[52:57]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[58:60]]

    coverage = coverage.replace('\n', ' ', regex=True)
    coverage = coverage.replace(' ', ' ', regex=True)
    print("PCCE master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref"), "coverage" : coverage.set_index("Ref"), "rrp" : rrp.set_index("Ref"), "dashboard" : dashboard.set_index("Ref"), "financial_progress" : financial_progress.set_index("Ref"), "operational_achievements" : operational_achievements.set_index("Ref")}

def organize_sheets():
    bucket = {}
    bucket["DREF"] = organize_dref(spark.read.table("dref").toPandas())
    bucket["EA"] = organize_ea(spark.read.table("ea").toPandas())
    bucket["MCMR"] = organize_mcmr(spark.read.table("mcmr").toPandas())
    bucket["PCCE"] = organize_protracted(spark.read.table("pcce").toPandas())
    return bucket


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def full_list(cols):
    output = []
    if not isinstance(cols, list):
        cols = [cols]
    for col in cols:
        output.append(col)
        output.append(f"{col} (days)")
        output.append(f"{col} date")
        output.append(f"{col} expected date")
    return output


def summarize_df(df):
    df = df.copy()
    categories = ["Achieved", "Not Achieved", "Achieved Early", "Achieved Late", "DNU", "Upcoming"]

    for category in categories:
        df.loc[:, category] = df.apply(lambda x: sum(str(cell) == category for cell in x), axis=1)
    
    df.loc[:, "Data Completeness"] = (df["Achieved"] + df["Achieved Early"] + df["Achieved Late"]) / \
                                    (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"])
    numerator = ((df["Achieved"] + df["Achieved Early"]) * 2) + df["Achieved Late"]
    denominator = (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 2

    df["General Performance"] = np.where(denominator != 0, numerator / denominator, 0)

    cols_to_move = ["Achieved", "Not Achieved", "Upcoming", "Achieved Early", "Achieved Late", "DNU", "Data Completeness", "General Performance"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]
    return df


def update_general_info(folder, general):
    df = general.copy()
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns={df.columns[1]: "Trigger Date"}, inplace=True)
    for name, temp in folder.items():
        df["Achieved"] = temp["Achieved"] if "Achieved" not in df.columns else df["Achieved"] + temp["Achieved"]
        df["Not Achieved"] = temp["Not Achieved"] if "Not Achieved" not in df.columns else df["Not Achieved"] + temp["Not Achieved"]
        df["Upcoming"] = temp["Upcoming"] if "Upcoming" not in df.columns else df["Upcoming"] + temp["Upcoming"]
        df["Achieved Early"] = temp["Achieved Early"] if "Achieved Early" not in df.columns else df["Achieved Early"] + temp["Achieved Early"]
        df["Achieved Late"] = temp["Achieved Late"] if "Achieved Late" not in df.columns else df["Achieved Late"] + temp["Achieved Late"]
        df["DNU"] = temp["DNU"] if "DNU" not in df.columns else df["DNU"] + temp["DNU"]
    
    df.loc[:, "Data Completeness"] = (df["Achieved"] + df["Achieved Early"] + df["Achieved Late"]) / \
                                    (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"])
    numerator = ((df["Achieved"] + df["Achieved Early"]) * 2) + df["Achieved Late"]
    denominator = (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 2

    df["General Performance"] = np.where(denominator != 0, numerator / denominator, 0)

    return df



'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def area_split_ea(overview, columns, general):
    msr_column = "MSR ready (compliant or resource allocated)"
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:16] + [columns[22]])] # add EA coverage
    surge = overview[full_list(columns[23:26])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[38:40])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[40:44])] # Upcoming joint statement in master data
    logistics = overview[full_list(columns[44:47])]
    im = overview[full_list(columns[47:52])]
    finance = overview[full_list(columns[52:56])]
    program_delivery = overview[full_list(columns[56:58])]
    security = overview[[msr_column, f"{msr_column} (days)"]]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(im)
    areas["Finance"] = summarize_df(finance)
    areas["Security"] = summarize_df(security)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    areas["Program Delivery"] = summarize_df(program_delivery)
    return areas

def area_split_dref(overview, columns, general):
    msr_column = "MSR ready (compliant or resource allocated)"

    assessment = overview[full_list(columns[11])]
    risk = overview[full_list(columns[12:14])]
    resource_mobilization = overview[full_list(columns[14:16])]
    surge = overview[full_list(columns[16:19])]
    logistics = overview[full_list(columns[32:34])]
    finance = overview[full_list(columns[34:38])]
    delivery = overview[full_list([columns[38]])] # add targeted population, ehi distribution, and implementation rate
    security = overview[[msr_column, f"{msr_column} (days)"]]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["Risk"] = summarize_df(risk)
    areas["Logistics"] = summarize_df(logistics)
    areas["Finance"] = summarize_df(finance)
    areas["Delivery"] = summarize_df(delivery) 
    areas["Security"] = summarize_df(security)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas

def area_split_mcmr(overview, columns, general):
    resource_mobilization = overview[full_list(columns[11:14] + [columns[38]])] # add coverage
    surge = overview[full_list(columns[20:22])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[35:37])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[37])]
    logistics = overview[full_list(columns[38:41])]
    im = overview[full_list(columns[41:42])]
    finance = overview[full_list(columns[42:44])]

    areas = {}
    
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(im)
    areas["Finance"] = summarize_df(finance)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas


def area_split_pcce(overview, columns, general):
    msr_column = "MSR ready (compliant or resource allocated)"

    assessment = overview[columns[11:12]]
    resource_mobilization = overview[full_list(columns[12:15] + [columns[21]])]
    surge = overview[full_list(columns[22:25])]
    hr = overview[full_list(columns[39:41])]
    coordination = overview[full_list(columns[41:44])]
    logistics = overview[full_list(columns[44:47])]
    im = overview[full_list(columns[47:52])]
    finance = overview[full_list(columns[52:56])]
    # delivery = overview[full_list(columns[55:57])] # add percentage of targeted population receiving assistance and % of planned budget implementation
    security = overview[[msr_column, f"{msr_column} (days)"]]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(im)
    areas["Finance"] = summarize_df(finance)
    areas["Security"] = summarize_df(security)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def convert_date(date_str):
    if date_str in ["-", "DNU", "Not Achieved"] or pd.isna(date_str):
        return date_str

    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%m/%d/%Y", "%d/%m/%Y"]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str)[:10], fmt)
        except:
            continue
    
    return date_str


def determine_status(row, limit):
    keys = row.index.tolist()
    r0, r1 = row.iloc[0], row.iloc[1]
    expected_date = r0 + pd.Timedelta(limit)
    if r1 == "-" or pd.isna(r1):
        deadline = r0 + pd.Timedelta(days=limit)
        if deadline > datetime.now():
            return pd.Series(["Upcoming", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"]) 
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if r1 == "DNU":
        return pd.Series(["DNU", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    if r1 == "Not Achieved":
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    days = (r1 - r0).days
    delta = days - limit
    if days > limit:
        return pd.Series(["Achieved Late", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if delta == 0:
        return pd.Series(["Achieved", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])

    return pd.Series(["Achieved Early", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])

def msr_ready(row ,limit):
    msr_column = "MSR ready (compliant or resource allocated)"
    r0, r1, r2 = row.iloc[0], row.iloc[1], row.iloc[2]
    deadline = r0 + pd.Timedelta(days=limit)

    if (pd.isna(r1) or r1 == "-" or r1 == "DNU" or r1 == "Not Achieved") and (pd.isna(r2) or r2 == "-" or r2 == "DNU" or r2 == "Not Achieved"):        
        if deadline > datetime.now():
            return pd.Series(["Upcoming", 365, "-", deadline], index=[msr_column, f"{msr_column} (days)", f"{msr_column} expected date"])
        return pd.Series(["Not Achieved", 365, "-", deadline], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date", f"{msr_column} expected date"])
    elif pd.isna(r1) or r1 == "-" or r1 == "DNU" or r1 == "Not Achieved":
        days = (r2 - r0).days
        delta = days - limit
        return pd.Series(["Achieved Late" if days > limit else "Achieved Early", delta, r2, deadline], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date", f"{msr_column} expected date"])
    else:
        days = (r1 - r0).days
        delta = days - limit
        return pd.Series(["Achieved Late" if days > limit else "Achieved Early", delta, r1, deadline], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date", f"{msr_column} expected date"])


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def process_ea(ea):
    op = ea["operational_progresses"].copy()
    if op.empty:
        return None
    dash = ea["dashboard_progress"].copy()
    fin = ea["financial_progress"].copy()
    nfi = ea["nfi"].copy()
    key = "achievements"
    msr_column = "MSR ready (compliant or resource allocated)"

    ea[key] = pd.DataFrame()
    if "Trigger Date_" in ea["disasters"].columns:
        ea["disasters"] = ea["disasters"].rename(columns={"Trigger Date_":"Trigger_Date_"})
    start_date = ea["disasters"]["Trigger_Date_"].apply(convert_date)

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
    deltas = [3, 3, 0, 4, 11, 18, 1, 2, 3, 11, 13, 2, 4, 7, 2, 7]

    for i in range(16):
        ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
    
    ea[key][[msr_column, f"{msr_column} (days)", f"{msr_column} date", f"{msr_column} expected date"]] = pd.merge(start_date, op[[on[16], on[17]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    ea[key][[nn[0], f"{nn[0]} (days)", f"{nn[0]} date", f"{nn[0]} expected date"]] = pd.merge(start_date, nfi[nn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[nn[1], f"{nn[1]} (days)", f"{nn[1]} date", f"{nn[1]} expected date"]] = pd.merge(start_date, nfi[nn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[nn[2], f"{nn[2]} (days)", f"{nn[2]} date", f"{nn[2]} expected date"]] = pd.merge(start_date, nfi[nn[2]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    
    ea[key][[dn[0], f"{dn[0]} (days)", f"{dn[0]} date", f"{dn[0]} expected date"]] = pd.merge(start_date, dash[dn[0]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[dn[1], f"{dn[1]} (days)", f"{dn[1]} date", f"{dn[1]} expected date"]] = pd.merge(start_date, dash[dn[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[dn[2], f"{dn[2]} (days)", f"{dn[2]} date", f"{dn[2]} expected date"]] = pd.merge(start_date, dash[dn[2]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[dn[4], f"{dn[4]} (days)", f"{dn[4]} date", f"{dn[4]} expected date"]] = pd.merge(start_date, dash[dn[4]], left_index=True, right_index=True).apply(determine_status, args=(30,), axis=1)
    ea[key][[dn[3], f"{dn[3]} (days)", f"{dn[3]} date", f"{dn[3]} expected date"]] = pd.merge(start_date, dash[dn[3]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[fn[0], f"{fn[0]} (days)", f"{fn[0]} date", f"{fn[0]} expected date"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[fn[1], f"{fn[1]} (days)", f"{fn[1]} date", f"{fn[1]} expected date"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[fn[2], f"{fn[2]} (days)", f"{fn[2]} date", f"{fn[2]} expected date"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(16,), axis=1)
    ea[key][[fn[3], f"{fn[3]} (days)", f"{fn[3]} date", f"{fn[3]} expected date"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(20,), axis=1)
    ea[key][[fn[4], f"{fn[4]} (days)", f"{fn[4]} date", f"{fn[4]} expected date"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)

    return ea

def process_dref(dref):
    op = dref["operational_progresses"].copy()
    
    if op.empty:
        return None
    fin = dref["financial_progress"].copy()
    key = "achievements"
    msr_column = "MSR ready (compliant or resource allocated)"
    dref[key] = pd.DataFrame()
    start_date = dref["disasters"]["Trigger_Date_"].apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    fn = fin.columns
    for col in fn[:5]:
        fin[col] = fin[col].apply(convert_date)
    
    dref[key]["Ref"] = dref["disasters"].index
    dref[key].set_index("Ref", inplace=True)

    deltas = [3, 12, 14, 10, 14, 1, 2, 3, 19, 14]
    for i in range(10):
        dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    dref[key][[msr_column, f"{msr_column} (days)", f"{msr_column} date", f"{msr_column} expected date"]] = pd.merge(start_date, op[[on[10], on[11]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    
    deltas_fn = [17, 21, 22, 30, 30]
    for i in range(5):
        dref[key][[fn[i], f"{fn[i]} (days)", f"{fn[i]} date", f"{fn[i]} expected date"]] = pd.merge(start_date, fin[fn[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas_fn[i],), axis=1)

    return dref

def process_mcmr(mcmr):
    op = mcmr["operational_progresses"].copy()
    
    if op.empty:
        return None
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
    deltas = [3, 4, 11, 1, 2, 11, 13, 1, 11, 11, 14, 1]

    for i in range(12):
        mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    mcmr[key][[fn[0], f"{fn[0]} (days)", f"{fn[0]} date", f"{fn[0]} expected date"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(9,), axis=1)
    mcmr[key][[fn[1], f"{fn[1]} (days)", f"{fn[1]} date", f"{fn[1]} expected date"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)

    return mcmr
    

def process_pcce(pcce):
    op = pcce["operational_progresses"].copy()
    if op.empty:
        return None
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
    deltas = [3, 3, 4, 11, 18, 7, 2, 3, 11, 13, 2, 7, 1, 11, 11, 14, 28]
    for i in range(17):
        pcce[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    pcce[key][[msr_column, f"{msr_column} (days)", f"{msr_column} date", f"{msr_column} expected date"]] = pd.merge(start_date, op[[on[17], on[18]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    
    deltas_dn = [1, 3, 7, 14, 30]
    for i in range(5):
        pcce[key][[dn[i], f"{dn[i]} (days)", f"{dn[i]} date", f"{dn[i]} expected date"]] = pd.merge(start_date, dash[dn[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas_dn[i],), axis=1)


    deltas_fn = [11, 14, 16, 20, 32]
    for i in range(5):
        pcce[key][[fn[i], f"{fn[i]} (days)", f"{fn[i]} date", f"{fn[i]} expected date"]] = pd.merge(start_date, fin[fn[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas_fn[i],), axis=1)

    return pcce


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def generate_overview(bucket, sheets):
    ea = process_ea(bucket["EA"])
    dref = process_dref(bucket["DREF"])
    mcmr = process_mcmr(bucket["MCMR"])
    pcce = process_pcce(bucket["PCCE"])

    key = "achievements"
    split_dict = {}
    for sheet_name, sheet in sheets.items():
        if "EA" in sheet_name and ea != None:
            split_dict["EA"] = area_split_ea(ea[key], sheet.columns.tolist(), bucket["EA"]["disasters"])
        elif "DREF" in sheet_name and dref != None:
            split_dict["DREF"] = area_split_dref(dref[key], sheet.columns.tolist(), bucket["DREF"]["disasters"])
        elif "MCMR" in sheet_name and mcmr != None:
            split_dict["MCMR"] = area_split_mcmr(mcmr[key], sheet.columns.tolist(), bucket["MCMR"]["disasters"])
        elif "Protracted" in sheet_name and pcce != None:
            split_dict["PCCE"] = area_split_pcce(pcce[key], sheet.columns.tolist(), bucket["PCCE"]["disasters"])
        else:
            continue

    print("All complete")
    
    return split_dict




'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

def read_area_info_folder(dfs):
    cols = ["Ref", "Area", "Achieved", "Not Achieved", "Upcoming", "Achieved Early", "Achieved Late", "DNU", "Data Completeness", "General Performance"]
    df_list = []
    for key, df in dfs.items():
        df.reset_index(inplace=True)
        df["Area"] = key
        if key == "General Information":
            continue
        df = df[cols]
        df_list.append(df)
    if len(df_list) == 0:
        return pd.DataFrame(columns=cols)
    else:    
        df_combined = pd.concat(df_list)
        return df_combined


def area_info(area_split_dfs):
    df = read_area_info_folder(area_split_dfs.get("EA", {}))
    df1 = read_area_info_folder(area_split_dfs.get("DREF", {}))
    df2 = read_area_info_folder(area_split_dfs.get("MCMR", {}))
    df3 = read_area_info_folder(area_split_dfs.get("PCCE", {}))
    df_combined = pd.concat([df, df1, df2, df3])
    
    return df_combined

'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def read_task_info(df, op, op_df, area):
    cols = df.columns[9:].copy()
    task_infos = []
    for index, row in df.iterrows():
        for a, b, c, d in zip(cols[::4], cols[1::4], cols[2::4], cols[3::4]):
            e = a.replace("_", " ")
            filtered_df = escalation[(escalation["Variant"] == op.upper()) & (escalation["Indicator"] == e)]
            column_name = op_df["Appeal_Name_"][index].split(" ")[:-1]
            column_name = "_".join(column_name)
            column_name = column_name + "__" +op_df["Appeal_Code"][index] + "_"
            if not filtered_df.empty and (column_name in filtered_df.columns or f"EA_{column_name}" in filtered_df.columns):
                escalated = filtered_df.loc[:, column_name].values[0]
            else:
                escalated = "Not Escalated"
            if pd.notna(row[a]) and row[a] == "DNU":
                delta = ""
            else:
                if row[b] == 365:
                    delta = ""
                else:
                    delta = row[b] * -1
            task_infos.append({
                "Ref" : row["Ref"],
                "EWTS Varient" : op,
                "Area" : area,
                "Task" : e,
                "Status" : row[a] if pd.notna(row[a]) else "Not Achieved",
                "Completed" : str(row[c])[:10],
                "Expected Date" : str(row[d])[:10],
                "Delta" : delta,
                "Escalated" : escalated,
            })
    return task_infos


def read_im(df, op, op_df):
    cols = df.columns[9:].copy()
    task_infos = []
    for index, row in df.iterrows():
        for a, b, c, d in zip(cols[::4], cols[1::4], cols[2::4], cols[3::4]):
            if op == "mcmr":
                filtered_df = escalation[(escalation["Variant"] == op.upper()) & (escalation["Indicator"] == "A multi country dashboard is in place and updated timely to display the situation and the activities being implemented")]
            else:
                filtered_df = escalation[(escalation["Variant"] == op.upper()) & (escalation["Indicator"] == "A dashboard is in place and updated timely to display the situation and the activities being implemented")]
            
            column_name = op_df["Appeal_Name_"][index].split(" ")[:-1]
            column_name = "_".join(column_name)
            column_name = column_name + "__" +op_df["Appeal_Code"][index] + "_"
            if not filtered_df.empty and (column_name in filtered_df.columns or f"EA_{column_name}" in filtered_df.columns):
                escalated = filtered_df.loc[:, column_name].values[0]
            else:
                escalated = "Not Escalated"
            if pd.notna(row[a]) and row[a] == "DNU":
                delta = ""
            else:
                if row[b] == 365:
                    delta = ""
                else:
                    delta = row[b] * -1
            task_infos.append({
                "Ref" : row["Ref"],
                "EWTS Varient" : op,
                "Area" : "Information Management",
                "Task" : a.replace("_", " "),
                "Status" : row[a] if pd.notna(row[a]) else "Not Achieved",
                "Completed" : str(row[c])[:10],
                "Expected Date": str(row[d])[:10],
                "Delta" : delta,
                "Escalated" : escalated,
            })
    return task_infos


status_mapping = {
    "Achieved" : 2,
    "Achieved Early" : 2,
    "Achieved Late" : 1,
    "DNU" : 0,
    "Upcoming" : 0,
    "Not Achieved" : 0,
}


def areas_in_op(adf, op):
    task_infos = []
    op_df = adf.get("General Information", pd.DataFrame())
    op_df = op_df[op_df.columns[:18]]
    if "Appeal_Name_" not in op_df.columns:
        op_df = op_df.rename(columns={"Appeal Name_": "Appeal_Name_", "Appeal Code":"Appeal_Code"})
    
    for key, df in adf.items():
        if key == "General Information":
            continue
        elif key == "Information Management":
            task_infos += read_im(df, op, op_df)
            continue
        task_infos += read_task_info(df, op, op_df, key)
    return task_infos


def task_info_extraction(area_split_dfs):
    task_infos = areas_in_op(area_split_dfs.get("EA", {}), "ea") + areas_in_op(area_split_dfs.get("DREF", {}), "dref") + areas_in_op(area_split_dfs.get("MCMR", {}), "mcmr") + areas_in_op(area_split_dfs.get("PCCE", {}), "protracted crisis")
    df = pd.DataFrame(task_infos)
    df["Avg"] = df["Status"].map(status_mapping)
    df_grouped = df.groupby(["EWTS Varient", "Task"], as_index=False).agg({
        'Avg': 'mean'
    })
    df = df.merge(df_grouped, on=["EWTS Varient", "Task"], suffixes=('', '_col'))
    df = df.drop(columns=["Avg"])
    df["Avg_col"] = df["Avg_col"] / 2
    df["Delta"] = df["Delta"].astype(str)
    return df



sheets = {}
sheets["EA"] = spark.read.table("ea").toPandas()
sheets["DREF"] = spark.read.table("dref").toPandas()
dref_cols = sheets["DREF"].columns
for col in dref_cols:
    new_col = col.replace(" ", "_")
    sheets["DREF"] = sheets["DREF"].rename(columns={col:new_col})
sheets["MCMR"] = spark.read.table("mcmr").toPandas()
sheets["Protracted"] = spark.read.table("pcce").toPandas()
escalation = spark.read.table("escalation_events").toPandas()
bucket = organize_sheets()
area_split_dfs = generate_overview(bucket, sheets)
general_df = pd.concat([i["General Information"] for _, i in area_split_dfs.items()])
general_df.reset_index(inplace=True)
general_df = general_df[['Ref'] + [col for col in general_df.columns[:19] if col != 'Ref']]
spark_general_info = spark.createDataFrame(general_df)
spark_general_info = spark_general_info.toDF(*[c.replace(" ", "_") for c in spark_general_info.columns])
spark_general_info = spark_general_info.withColumn("Trigger_Date", to_date("Trigger_Date", "M/d/yyyy"))
spark_general_info.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("master_data_processing.general_info")

df_area_info = area_info(area_split_dfs)

ti = task_info_extraction(area_split_dfs)

spark_task_infos = spark.createDataFrame(ti)
spark_task_infos = spark_task_infos.toDF(*[c.replace(" ", "_") for c in spark_task_infos.columns])
spark_task_infos.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.task_info")
df_area_info["General Performance"] = df_area_info["General Performance"].fillna(0)

spark_area_info = spark.createDataFrame(df_area_info)
spark_area_info = spark_area_info.toDF(*[c.replace(" ", "_") for c in spark_area_info.columns])
spark_area_info.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("master_data_processing.area_info")