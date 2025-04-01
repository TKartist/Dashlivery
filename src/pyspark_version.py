from datetime import datetime
import pandas as pd
from pyspark.sql.functions import to_date

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


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def full_list(cols):
    output = []
    if not isinstance(cols, list):
        cols = [cols]
    for col in cols:
        output.append(col)
        output.append(f"{col} (days)")
        output.append(f"{col} date")
    return output


def full_list_im(cols):
    output = []
    if not isinstance(cols, list):
        cols = [cols]
    for col in cols:
        output.append(col)
        output.append(f"{col} date")
    return output

def summarize_df(df):
    df = df.copy()
    categories = ["Achieved", "Not Achieved", "Achieved Early", "Achieved Late", "DNU", "Missing"]

    for category in categories:
        df.loc[:, category] = df.apply(lambda x: sum(str(cell) == category for cell in x), axis=1)
    
    df.loc[:, "Data Completeness"] = (df["Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Not Achieved"]) / \
                                    (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Missing"] + df["DNU"])
    df.loc[:, "General Performance"] = (((df["Achieved"] + df["Achieved Early"]) * 2) + df["Achieved Late"]) / \
                                    ((df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Missing"]) * 2)

    cols_to_move = ["Achieved", "Not Achieved", "Missing", "Achieved Early", "Achieved Late", "DNU", "Data Completeness", "General Performance"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]
    return df


def update_general_info(folder, general):
    df = general.copy()
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns={df.columns[1]: "Trigger Date"}, inplace=True)
    for name, temp in folder.items():
        df["Achieved"] = temp["Achieved"] if "Achieved" not in df.columns else df["Achieved"] + temp["Achieved"]
        df["Not Achieved"] = temp["Not Achieved"] if "Not Achieved" not in df.columns else df["Not Achieved"] + temp["Not Achieved"]
        df["Missing"] = temp["Missing"] if "Missing" not in df.columns else df["Missing"] + temp["Missing"]
        df["Achieved Early"] = temp["Achieved Early"] if "Achieved Early" not in df.columns else df["Achieved Early"] + temp["Achieved Early"]
        df["Achieved Late"] = temp["Achieved Late"] if "Achieved Late" not in df.columns else df["Achieved Late"] + temp["Achieved Late"]
        df["DNU"] = temp["DNU"] if "DNU" not in df.columns else df["DNU"] + temp["DNU"]
    
    df.loc[:, "Data Completeness"] = (df["Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Not Achieved"]) / \
                                    (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"] + df["Missing"] + df["DNU"])
    df.loc[:, "General Performance"] = (((df["Achieved"] + df["Achieved Early"]) * 2) + df["Achieved Late"]) / \
                                    ((df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 2)

    return df



'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def area_split_ea(overview, columns, general):
    msr_column = "MSR ready (compliant or resource allocated)"
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:16] + [columns[22]])] # add EA coverage
    surge = overview[full_list(columns[23:26])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[38:40])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[40:44])] # missing joint statement in master data
    logistics = overview[full_list(columns[44:47])]
    im = overview[full_list_im(columns[47:52])]
    finance = overview[full_list(columns[52:56])]
    security = overview[[msr_column, f"{msr_column} (days)"]]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Resource Mobilization"] = summarize_df(resource_mobilization)
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
    areas["Resource Mobilization"] = summarize_df(resource_mobilization)
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
    im = overview[full_list_im(columns[41:42])]
    finance = overview[full_list(columns[42:44])]

    areas = {}
    
    areas["Resource Mobilization"] = summarize_df(resource_mobilization)
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
    im = overview[full_list_im(columns[47:52])]
    finance = overview[full_list(columns[52:56])]
    # delivery = overview[full_list(columns[55:57])] # add percentage of targeted population receiving assistance and % of planned budget implementation
    security = overview[[msr_column, f"{msr_column} (days)"]]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Resource Mobilization"] = summarize_df(resource_mobilization)
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
    if r1 == "-" or pd.isna(r1):
        deadline = r0 + pd.Timedelta(days=limit)
        if deadline > datetime.now():
            return pd.Series(["Missing", 365, "-"], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date"]) 
        return pd.Series(["Not Achieved", 365, "-"], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date"])
    if r1 == "DNU":
        return pd.Series(["DNU", 365, "-"], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date"])
    
    if r1 == "Not Achieved":
        return pd.Series(["Not Achieved", 365, "-"], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date"])
    
    days = (r1 - r0).days
    delta = days - limit
    if days > limit:
        return pd.Series(["Achieved Late", delta, r1], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date"])

    return pd.Series(["Achieved Early", delta, r1], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date"])

def determine_done(row):
    if row == "-":
        return ["Missing", "-"]
    if row == "DNU":
        return ["DNU", "-"]
    if row == "Not Achieved":
        return ["Not Achieved", "-"]
    return ["Achieved", row]

def msr_ready(row ,limit):
    msr_column = "MSR ready (compliant or resource allocated)"
    r0, r1, r2 = row.iloc[0], row.iloc[1], row.iloc[2]
    deadline = r0 + pd.Timedelta(days=limit)

    if (pd.isna(r1) or r1 == "-" or r1 == "DNU" or r1 == "Not Achieved") and (pd.isna(r2) or r2 == "-" or r2 == "DNU" or r2 == "Not Achieved"):        
        if deadline > datetime.now():
            return pd.Series(["Missing", 365, "-"], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date"])
        return pd.Series(["Not Achieved", 365, "-"], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date"])
    elif pd.isna(r1) or r1 == "-" or r1 == "DNU" or r1 == "Not Achieved":
        days = (r2 - r0).days
        delta = days - limit
        return pd.Series(["Achieved Late" if days > limit else "Achieved Early", delta, r2], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date"])
    else:
        days = (r1 - r0).days
        delta = days - limit
        return pd.Series(["Achieved Late" if days > limit else "Achieved Early", delta, r1], index=[msr_column, f"{msr_column} (days)", f"{msr_column} date"])


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
    ea[key][[on[0], f"{on[0]} (days)", f"{on[0]} date"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[on[1], f"{on[1]} (days)", f"{on[1]} date"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(0,), axis=1)
    ea[key][[on[2], f"{on[2]} (days)", f"{on[2]} date"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[on[3], f"{on[3]} (days)", f"{on[3]} date"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    ea[key][[on[4], f"{on[4]} (days)", f"{on[4]} date"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[on[5], f"{on[5]} (days)", f"{on[5]} date"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(18,), axis=1)
    ea[key][[on[6], f"{on[6]} (days)", f"{on[6]} date"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    ea[key][[on[7], f"{on[7]} (days)", f"{on[7]} date"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[8], f"{on[8]} (days)", f"{on[8]} date"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    ea[key][[on[9], f"{on[9]} (days)", f"{on[9]} date"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[on[10], f"{on[10]} (days)", f"{on[10]} date"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(13,), axis=1)
    ea[key][[on[11], f"{on[11]} (days)", f"{on[11]} date"]] = pd.merge(start_date, op[on[11]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[12], f"{on[12]} (days)", f"{on[12]} date"]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    ea[key][[on[13], f"{on[13]} (days)", f"{on[13]} date"]] = pd.merge(start_date, op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[on[14], f"{on[14]} (days)", f"{on[14]} date"]] = pd.merge(start_date, op[on[14]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    ea[key][[on[15], f"{on[15]} (days)", f"{on[15]} date"]] = pd.merge(start_date, op[on[15]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    ea[key][[msr_column, f"{msr_column} (days)", f"{msr_column} date"]] = pd.merge(start_date, op[[on[16], on[17]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    ea[key][[nn[0], f"{nn[0]} (days)", f"{nn[0]} date"]] = pd.merge(start_date, nfi[nn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[nn[1], f"{nn[1]} (days)", f"{nn[1]} date"]] = pd.merge(start_date, nfi[nn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][dn[0]] = dash[dn[0]].apply(lambda x: determine_done(x)[0])
    ea[key][f"{dn[0]} date"] = dash[dn[0]].apply(lambda x: determine_done(x)[1])
    ea[key][dn[1]] = dash[dn[1]].apply(lambda x: determine_done(x)[0])
    ea[key][f"{dn[1]} date"] = dash[dn[1]].apply(lambda x: determine_done(x)[1])
    ea[key][dn[2]] = dash[dn[2]].apply(lambda x: determine_done(x)[0])
    ea[key][f"{dn[2]} date"] = dash[dn[2]].apply(lambda x: determine_done(x)[1])
    ea[key][dn[3]] = dash[dn[4]].apply(lambda x: determine_done(x)[0])
    ea[key][f"{dn[3]} date"] = dash[dn[4]].apply(lambda x: determine_done(x)[1])
    ea[key][dn[4]] = dash[dn[4]].apply(lambda x: determine_done(x)[0])
    ea[key][f"{dn[4]} date"] = dash[dn[4]].apply(lambda x: determine_done(x)[1])
    ea[key][[fn[0], f"{fn[0]} (days)", f"{fn[0]} date"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    ea[key][[fn[1], f"{fn[1]} (days)", f"{fn[1]} date"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    ea[key][[fn[2], f"{fn[2]} (days)", f"{fn[2]} date"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(16,), axis=1)
    ea[key][[fn[3], f"{fn[3]} (days)", f"{fn[3]} date"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(20,), axis=1)
    ea[key][[fn[4], f"{fn[4]} (days)", f"{fn[4]} date"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)

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

    dref[key][[on[0], f"{on[0]} (days)", f"{on[0]} date"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    dref[key][[on[1], f"{on[1]} (days)", f"{on[1]} date"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(12,), axis=1)
    dref[key][[on[2], f"{on[2]} (days)", f"{on[2]} date"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    dref[key][[on[3], f"{on[3]} (days)", f"{on[3]} date"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(10,), axis=1) # check for viability (10 if NS started moving)
    dref[key][[on[4], f"{on[4]} (days)", f"{on[4]} date"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    dref[key][[on[5], f"{on[5]} (days)", f"{on[5]} date"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    dref[key][[on[6], f"{on[6]} (days)", f"{on[6]} date"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    dref[key][[on[7], f"{on[7]} (days)", f"{on[7]} date"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    dref[key][[on[8], f"{on[8]} (days)", f"{on[8]} date"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(19,), axis=1)
    dref[key][[on[9], f"{on[9]} (days)", f"{on[9]} date"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    dref[key][[msr_column, f"{msr_column} (days)", f"{msr_column} date"]] = pd.merge(start_date, op[[on[10], on[11]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    
    dref[key][[fn[0], f"{fn[0]} (days)", f"{fn[0]} date"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(17,), axis=1)
    dref[key][[fn[1], f"{fn[1]} (days)", f"{fn[1]} date"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(21,), axis=1)
    dref[key][[fn[2], f"{fn[2]} (days)", f"{fn[2]} date"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(22,), axis=1)
    dref[key][[fn[3], f"{fn[3]} (days)", f"{fn[3]} date"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(30,), axis=1)
    dref[key][[fn[4], f"{fn[4]} (days)", f"{fn[4]} date"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(30,), axis=1)

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

    mcmr[key][[on[0], f"{on[0]} (days)", f"{on[0]} date"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    mcmr[key][[on[1], f"{on[1]} (days)", f"{on[1]} date"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    mcmr[key][[on[2], f"{on[2]} (days)", f"{on[2]} date"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    mcmr[key][[on[3], f"{on[3]} (days)", f"{on[3]} date"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1) # check for viability (10 if NS started moving)
    mcmr[key][[on[4], f"{on[4]} (days)", f"{on[4]} date"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    mcmr[key][[on[5], f"{on[5]} (days)", f"{on[5]} date"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    mcmr[key][[on[6], f"{on[6]} (days)", f"{on[6]} date"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(13,), axis=1)
    mcmr[key][[on[7], f"{on[7]} (days)", f"{on[7]} date"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    mcmr[key][[on[8], f"{on[8]} (days)", f"{on[8]} date"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    mcmr[key][[on[9], f"{on[9]} (days)", f"{on[9]} date"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    mcmr[key][[on[10], f"{on[10]} (days)", f"{on[10]} date"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    mcmr[key][on[11]] = op[on[0]].apply(lambda x: determine_done(x)[0])
    mcmr[key][f"{on[11]} date"] = op[on[0]].apply(lambda x: determine_done(x)[1])
    mcmr[key][[fn[0], f"{fn[0]} (days)", f"{fn[0]} date"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(9,), axis=1)
    mcmr[key][[fn[1], f"{fn[1]} (days)", f"{fn[1]} date"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)

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
    pcce[key][[on[0], f"{on[0]} (days)", f"{on[0]} date"]] = pd.merge(start_date, op[on[0]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[on[1], f"{on[1]} (days)", f"{on[1]} date"]] = pd.merge(start_date, op[on[1]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[on[2], f"{on[2]} (days)", f"{on[2]} date"]] = pd.merge(start_date, op[on[2]], left_index=True, right_index=True).apply(determine_status, args=(4,), axis=1)
    pcce[key][[on[3], f"{on[3]} (days)", f"{on[3]} date"]] = pd.merge(start_date, op[on[3]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[4], f"{on[4]} (days)", f"{on[4]} date"]] = pd.merge(start_date, op[on[4]], left_index=True, right_index=True).apply(determine_status, args=(18,), axis=1)
    pcce[key][[on[5], f"{on[5]} (days)", f"{on[5]} date"]] = pd.merge(start_date, op[on[5]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[on[6], f"{on[6]} (days)", f"{on[6]} date"]] = pd.merge(start_date, op[on[6]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    pcce[key][[on[7], f"{on[7]} (days)", f"{on[7]} date"]] = pd.merge(start_date, op[on[7]], left_index=True, right_index=True).apply(determine_status, args=(3,), axis=1)
    pcce[key][[on[8], f"{on[8]} (days)", f"{on[8]} date"]] = pd.merge(start_date, op[on[8]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[9], f"{on[9]} (days)", f"{on[9]} date"]] = pd.merge(start_date, op[on[9]], left_index=True, right_index=True).apply(determine_status, args=(13,), axis=1)
    pcce[key][[on[10], f"{on[10]} (days)", f"{on[10]} date"]] = pd.merge(start_date, op[on[10]], left_index=True, right_index=True).apply(determine_status, args=(2,), axis=1)
    pcce[key][[on[11], f"{on[11]} (days)", f"{on[11]} date"]] = pd.merge(start_date, op[on[11]], left_index=True, right_index=True).apply(determine_status, args=(7,), axis=1)
    pcce[key][[on[12], f"{on[12]} (days)", f"{on[12]} date"]] = pd.merge(start_date, op[on[12]], left_index=True, right_index=True).apply(determine_status, args=(1,), axis=1)
    pcce[key][[on[13], f"{on[13]} (days)", f"{on[13]} date"]] = pd.merge(start_date, op[on[13]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[14], f"{on[14]} (days)", f"{on[14]} date"]] = pd.merge(start_date, op[on[14]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[on[15], f"{on[15]} (days)", f"{on[15]} date"]] = pd.merge(start_date, op[on[15]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    pcce[key][[on[16], f"{on[16]} (days)", f"{on[16]} date"]] = pd.merge(start_date, op[on[16]], left_index=True, right_index=True).apply(determine_status, args=(28,), axis=1)
    pcce[key][[msr_column, f"{msr_column} (days)", f"{msr_column} date"]] = pd.merge(start_date, op[[on[17], on[18]]], left_index=True, right_index=True).apply(msr_ready, args=(7,), axis=1)
    pcce[key][dn[0]] = dash[dn[0]].apply(lambda x: determine_done(x)[0])
    pcce[key][f"{dn[0]} date"] = dash[dn[0]].apply(lambda x: determine_done(x)[1])
    pcce[key][dn[1]] = dash[dn[1]].apply(lambda x: determine_done(x)[0])
    pcce[key][f"{dn[1]} date"] = dash[dn[1]].apply(lambda x: determine_done(x)[1])
    pcce[key][dn[2]] = dash[dn[2]].apply(lambda x: determine_done(x)[0])
    pcce[key][f"{dn[2]} date"] = dash[dn[2]].apply(lambda x: determine_done(x)[1])
    pcce[key][dn[3]] = dash[dn[3]].apply(lambda x: determine_done(x)[0])
    pcce[key][f"{dn[3]} date"] = dash[dn[3]].apply(lambda x: determine_done(x)[1])
    pcce[key][dn[4]] = dash[dn[4]].apply(lambda x: determine_done(x)[0])
    pcce[key][f"{dn[4]} date"] = dash[dn[4]].apply(lambda x: determine_done(x)[1])
    pcce[key][[fn[0], f"{fn[0]} (days)", f"{fn[0]} date"]] = pd.merge(start_date, fin[fn[0]], left_index=True, right_index=True).apply(determine_status, args=(11,), axis=1)
    pcce[key][[fn[1], f"{fn[1]} (days)", f"{fn[1]} date"]] = pd.merge(start_date, fin[fn[1]], left_index=True, right_index=True).apply(determine_status, args=(14,), axis=1)
    pcce[key][[fn[2], f"{fn[2]} (days)", f"{fn[2]} date"]] = pd.merge(start_date, fin[fn[2]], left_index=True, right_index=True).apply(determine_status, args=(16,), axis=1)
    pcce[key][[fn[3], f"{fn[3]} (days)", f"{fn[3]} date"]] = pd.merge(start_date, fin[fn[3]], left_index=True, right_index=True).apply(determine_status, args=(20,), axis=1)
    pcce[key][[fn[4], f"{fn[4]} (days)", f"{fn[4]} date"]] = pd.merge(start_date, fin[fn[4]], left_index=True, right_index=True).apply(determine_status, args=(32,), axis=1)

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
    return split_dict
    print("All complete")


sheets = {}
sheets["EA"] = spark.read.table("ea").toPandas()
sheets["DREF"] = spark.read.table("dref").toPandas()
sheets["MCMR"] = spark.read.table("mcmr").toPandas()
sheets["Protracted"] = spark.read.table("pcce").toPandas()
escalation = spark.read.table("escalation_events").toPandas()
bucket = organize_sheets()
area_split_dfs = generate_overview(bucket, sheets)
general_df = pd.concat([i["General Information"] for _, i in area_split_dfs.items()])
general_df.reset_index(inplace=True)
general_df = general_df[['Ref'] + [col for col in general_df.columns if col != 'Ref']]
spark_general_info = spark.createDataFrame(general_df)
spark_general_info = spark_general_info.toDF(*[c.replace(" ", "_") for c in spark_general_info.columns])
spark_general_info = spark_general_info.withColumn("Trigger_Date", to_date("Trigger_Date", "M/d/yyyy"))
spark_general_info.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("master_data_processing.general_info")

'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

def read_area_info_folder(dfs):
    cols = ["Ref", "Area", "Achieved", "Not Achieved", "Missing", "Achieved Early", "Achieved Late", "DNU", "Data Completeness", "General Performance"]
    df_list = []
    print(cols)
    for key, df in dfs.items():
        df.reset_index(inplace=True)
        df["Area"] = key
        print(key)
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
        for a, b, c in zip(cols[::3], cols[1::3], cols[2::3]):
            d = a.replace("_", " ")
            filtered_df = escalation[(escalation["Variant"] == op.upper()) & (escalation["Indicator"] == d)]
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
                delta = row[b] * -1
            task_infos.append({
                "Ref" : row["Ref"],
                "EWTS Varient" : op,
                "Area" : area,
                "Task" : d,
                "Status" : row[a] if pd.notna(row[a]) else "Not Achieved",
                "Completed" : str(row[c])[:10],
                "Delta" : delta,
                "Escalated" : escalated,
            })
    return task_infos


def read_im(df, op, op_df):
    cols = df.columns[9:].copy()
    task_infos = []
    for index, row in df.iterrows():
        for a, b in zip(cols[::2], cols[1::2]):
            c = a.replace("_", " ")
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
            
            task_infos.append({
                "Ref" : row["Ref"],
                "EWTS Varient" : op,
                "Area" : "Information Management",
                "Task" : c,
                "Status" : row[a] if pd.notna(row[a]) else "Not Achieved",
                "Completed" : str(row[b])[:10],
                "Delta" :"",
                "Escalated" : escalated,
            })
    return task_infos


status_mapping = {
    "Achieved" : 2,
    "Achieved Early" : 2,
    "Achieved Late" : 1,
    "DNU" : 0,
    "Missing" : 0,
    "Not Achieved" : 0,
}


def areas_in_op(adf, op):
    task_infos = []
    op_df = adf.get("General Information", pd.DataFrame())
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



'''====================================================================================='''

print(area_split_dfs)
df_area_info = area_info(area_split_dfs)


ti = task_info_extraction(area_split_dfs)
spark_task_infos = spark.createDataFrame(ti)
spark_task_infos = spark_task_infos.toDF(*[c.replace(" ", "_") for c in spark_task_infos.columns])
spark_task_infos.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("master_data_processing.task_info")
print(df_area_info.columns)
df_area_info["General Performance"] = df_area_info["General Performance"].fillna(0)
for i in df_area_info["General Performance"]:
    print(i)

spark_area_info = spark.createDataFrame(df_area_info)
spark_area_info = spark_area_info.toDF(*[c.replace(" ", "_") for c in spark_area_info.columns])
spark_area_info.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("master_data_processing.area_info")
# spark.sql("DROP TABLE IF EXISTS ea")
# spark.sql("DROP TABLE IF EXISTS dref")
# spark.sql("DROP TABLE IF EXISTS escalation_events")
# spark.sql("DROP TABLE IF EXISTS mcmr")
# spark.sql("DROP TABLE IF EXISTS pcce")


