from datetime import datetime, timedelta, date
import pandas as pd
from pyspark.sql.functions import to_date, when, col
import warnings
import numpy as np
warnings.filterwarnings("ignore")

def organize_ea(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "EA" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11] + [col_name[77]]]
    ops_details = sheet.loc[:, [col_name[0]] + col_name[11:16] + col_name[22:26] + col_name[38:58] + col_name[69:72] + col_name[73:75]]
    dref_shift = sheet.loc[:, [col_name[0]] + [col_name[72]]]
    print("EA master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : ops_details.set_index("Ref"), "dref_shift" : dref_shift.set_index("Ref")}

def organize_dref(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11] + [col_name[54]]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:19] + col_name[32:39] + col_name[51:54]]

    print("DREF master data organized")
    return {"disasters": disasters.set_index("Ref"), "operational_progresses": operational_progresses.set_index("Ref")}

def organize_dref_escalated(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[4]] + sheet[col_name[6]]
    sheet["EWTS Varient_"] = "DREF 2nd Allocation"
    disasters = sheet.loc[:, col_name[:11] + [col_name[50]]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:17] + col_name[30:38]]

    print("DREF master data organized")
    return {"disasters": disasters.set_index("Ref"), "operational_progresses": operational_progresses.set_index("Ref")}

def organize_mcmr(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "MCMR" + sheet[col_name[6]] 
    disasters = sheet.loc[:, col_name[:11] + [col_name[52]]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:14] + col_name[20:22] + col_name[35:44] + col_name[46:]]

    print("MCMR master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref")}


def organize_protracted(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "PCCE" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:15] + col_name[21:25] + col_name[39:57] + col_name[57:58] + col_name[60:65]]

    print("PCCE master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref")}

def organize_sheets():
    bucket = {}
    bucket["DREF"] = organize_dref(spark.read.table("dref").toPandas())
    bucket["EA"] = organize_ea(spark.read.table("ea").toPandas())
    bucket["MCMR"] = organize_mcmr(spark.read.table("mcmr").toPandas())
    bucket["PCCE"] = organize_protracted(spark.read.table("pcce").toPandas())
    bucket["DREF_ESCALATED"] = organize_dref_escalated(spark.read.table("dref_2").toPandas())
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


def replace_category(row, df_classification):
    idx = row.name
    if df_classification[idx] == 'Orange':
        return row.apply(lambda x: 'N/A' if x == 'Not Achieved' else x)
    return row


def correct_im(df, df_classification):
    df = df.copy()
    df = df.apply(lambda row: replace_category(row, df_classification), axis=1)

    return df


def summarize_df(df):
    df = df.copy()
    categories = ["Achieved", "Not Achieved", "Achieved Early", "Achieved Late", "N/A", "Upcoming"]

    for category in categories:
        df.loc[:, category] = df.apply(lambda x: sum(str(cell) == category for cell in x), axis=1)
    
    dc_num = df["Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    dc_den = df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    df["Data Completeness"] = np.where(dc_den != 0, dc_num / dc_den, 1)

    numerator = df["Achieved"] * 3 + df["Achieved Early"] * 4 + df["Achieved Late"] * 2
    denominator = (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 4
    df["General Performance"] = np.where(denominator != 0, numerator / denominator, 0)

    cols_to_move = ["Achieved", "Not Achieved", "Upcoming", "Achieved Early", "Achieved Late", "N/A", "Data Completeness", "General Performance"]
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
        df["N/A"] = temp["N/A"] if "N/A" not in df.columns else df["N/A"] + temp["N/A"]
    
    dc_num = df["Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    dc_den = df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    df["Data Completeness"] = np.where(dc_den != 0, dc_num / dc_den, 1)
    
    numerator = df["Achieved"] * 3 + df["Achieved Early"] * 4 + df["Achieved Late"] * 2
    denominator = (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 4
    df["General Performance"] = np.where(denominator != 0, numerator / denominator, 0)

    return df



'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def area_split_ea(overview, columns, general):
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:16] + [columns[22]])] # add EA coverage
    surge = overview[full_list(columns[23:26])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[38:40])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[40:44])] # Upcoming joint statement in master data
    logistics = overview[full_list(columns[44:47])]
    im = overview[full_list(columns[47:52] + columns[73:75])]
    finance = overview[full_list(columns[52:56] + [columns[71]])]
    program_delivery = overview[full_list(columns[56:58])]
    security = overview[full_list(columns[69:71])]

    areas = {}
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(correct_im(im, general["Classification_"]))
    areas["Finance"] = summarize_df(finance)
    areas["Security"] = summarize_df(security)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    areas["Program Delivery"] = summarize_df(program_delivery)
    return areas

def area_split_dref_escalated(overview, columns, general):
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:14] + [columns[33]])]
    surge = overview[full_list(columns[14:17])]
    logistics = overview[full_list(columns[30:32])]
    finance = overview[full_list(columns[32:33] + columns[34:37])]
    delivery = overview[full_list([columns[37]])]

    if "Tracker Status" in general.columns:
        general = general.rename(columns={"Tracker Status" : "tracker Status"})

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["Logistics"] = summarize_df(logistics)
    areas["Finance"] = summarize_df(finance)
    areas["Program Delivery"] = summarize_df(delivery)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas

def area_split_dref(overview, columns, general):
    assessment = overview[full_list(columns[11])]
    risk = overview[full_list(columns[12:14])]
    resource_mobilization = overview[full_list(columns[14:16] + [columns[53]])]
    surge = overview[full_list(columns[16:19])]
    logistics = overview[full_list(columns[32:34])]
    finance = overview[full_list(columns[34:38])]
    delivery = overview[full_list([columns[38]])] # add targeted population, ehi distribution, and implementation rate
    security = overview[full_list(columns[51:53])]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["Risk"] = summarize_df(risk)
    areas["Logistics"] = summarize_df(logistics)
    areas["Finance"] = summarize_df(finance)
    areas["Program Delivery"] = summarize_df(delivery) 
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
    im = overview[full_list(columns[41:42] + columns[46:52])]
    finance = overview[full_list(columns[42:44])]

    areas = {}
    
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(correct_im(im, general["Classification_"]))
    areas["Finance"] = summarize_df(finance)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas


def area_split_pcce(overview, columns, general):
    assessment = overview[columns[11:12]]
    resource_mobilization = overview[full_list(columns[12:15] + [columns[21]] + [columns[62]])]
    surge = overview[full_list(columns[22:25])]
    hr = overview[full_list(columns[39:41])]
    coordination = overview[full_list(columns[41:44])]
    logistics = overview[full_list(columns[44:47])]
    im = overview[full_list(columns[47:52] + columns[63:65])]
    finance = overview[full_list(columns[52:56])]
    # delivery = overview[full_list(columns[55:57])] # add percentage of targeted population receiving assistance and % of planned budget implementation
    security = overview[full_list(columns[60:62])]

    areas = {}
    
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(correct_im(im, general["Classification_"]))
    areas["Finance"] = summarize_df(finance)
    areas["Security"] = summarize_df(security)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def convert_date(date_str):
    if date_str in ["-", "DNU", "Not Achieved", "N/A"] or pd.isna(date_str):
        return date_str

    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%m/%d/%Y", "%d/%m/%Y"]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str)[:10], fmt)
        except:
            continue
    
    return date_str


def determine_status_ea(row, l1, l2):
    keys = row.index.tolist()
    r0, r1 = row.iloc[0], row.iloc[1]
    if row.iloc[2] == "Yes":
        limit = l2
    else:
        limit = l1
    expected_date = r0 + pd.Timedelta(days=limit)
    if r1 == "-" or pd.isna(r1):
        deadline = r0 + pd.Timedelta(days=limit)
        if deadline > datetime.now():
            return pd.Series(["Upcoming", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"]) 
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if r1 == "DNU" or r1 == "N/A":
        return pd.Series(["N/A", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    if r1 == "Not Achieved":
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    if (limit == 30):
        limit = 31
    days = (r1 - r0).days
    delta = days - limit
    if days > limit:
        return pd.Series(["Achieved Late", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if delta == 0:
        return pd.Series(["Achieved", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])

    return pd.Series(["Achieved Early", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])

def determine_status(row, limit):
    keys = row.index.tolist()
    r0, r1 = row.iloc[0], row.iloc[1]
    expected_date = r0 + pd.Timedelta(days=limit)
    if r1 == "-" or pd.isna(r1):
        deadline = r0 + pd.Timedelta(days=limit)
        if deadline > datetime.now():
            return pd.Series(["Upcoming", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"]) 
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if r1 == "DNU" or r1 == "N/A":
        return pd.Series(["N/A", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    if r1 == "Not Achieved":
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    if (limit == 30):
        limit = 31
    days = (r1 - r0).days
    delta = days - limit
    if days > limit:
        return pd.Series(["Achieved Late", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if delta == 0:
        return pd.Series(["Achieved", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])

    return pd.Series(["Achieved Early", delta, r1, expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def process_ea(ea):
    op = ea["operational_progresses"].copy()
    if op.empty:
        return None
    key = "achievements"

    ea[key] = pd.DataFrame()
    if "Trigger Date_" in ea["disasters"].columns:
        ea["disasters"] = ea["disasters"].rename(columns={"Trigger Date_":"Trigger_Date_"})
    start_date = ea["disasters"]["Trigger_Date_"].apply(convert_date)
    start_date["EANigeriaMDRNG042"] = ea["disasters"]["Launch Date"]["EANigeriaMDRNG042"]
    surge_date = ea["disasters"]["Trigger_Date_"].copy()
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    ea[key]["Ref"] = ea["disasters"].index
    ea[key].set_index("Ref", inplace=True)
    deltas = [3, 3, 0, 4, 11, 18, 1, 2, 3, 11, 13, 2, 4, 7, 1, 11, 11, 14, 1, 3, 7, 14, 30, 11, 14, 16, 20, 32, 18, 7, 7, 6, 60, 90]
    deltas_b = [3, 3, 0, 0, 7, 14, 1, 2, 3, 7, 9, 2, 4, 7, 1, 7, 7, 10, 1, 3, 7, 14, 30, 7, 10, 12, 16, 29, 14, 7, 7, 2, 60, 90]

    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.concat([start_date, op[on[i]], ea["dref_shift"]], axis=1).apply(determine_status_ea, args=(deltas[i],deltas_b[i],), axis=1)
        else:
            ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.concat([start_date, op[on[i]], ea["dref_shift"]], axis=1).apply(determine_status_ea, args=(deltas[i],deltas_b[i],), axis=1)
    return ea

def process_dref(dref):
    op = dref["operational_progresses"].copy()
    
    if op.empty:
        return None
    
    key = "achievements"
    dref[key] = pd.DataFrame()
    start_date = dref["disasters"]["Trigger Date_"].apply(convert_date)
    surge_date = dref["disasters"]["Trigger Date_"].copy()
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    
    dref[key]["Ref"] = dref["disasters"].index
    dref[key].set_index("Ref", inplace=True)

    deltas = [3, 12, 14, 14, 14, 1, 2, 3, 19, 22, 17, 21, 22, 30, 30, 7, 7, 6]
    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:
            dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return dref

def process_dref_escalated(dref_escalated):
    op = dref_escalated["operational_progresses"].copy()
    
    if op.empty:
        return None
    
    key = "achievements"
    dref_escalated[key] = pd.DataFrame()
    start_date = dref_escalated["disasters"]["Trigger Date_"].apply(convert_date)
    surge_date = dref_escalated["disasters"]["Trigger Date_"].copy()
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    
    dref_escalated[key]["Ref"] = dref_escalated["disasters"].index
    dref_escalated[key].set_index("Ref", inplace=True)

    deltas = [3, 10, 10, 1, 2, 3, 17, 20, 17, 12, 20, 21, 24, 38]
    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            dref_escalated[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:
            dref_escalated[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return dref_escalated

def process_mcmr(mcmr):
    op = mcmr["operational_progresses"].copy()
    
    if op.empty:
        return None
    key = "achievements"

    mcmr[key] = pd.DataFrame()
    start_date = mcmr["disasters"][mcmr["disasters"].columns[1]].apply(convert_date)

    surge_date = mcmr["disasters"][mcmr["disasters"].columns[1]].copy()
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    
    mcmr[key]["Ref"] = mcmr["disasters"].index
    mcmr[key].set_index("Ref", inplace=True)
    deltas = [3, 4, 11, 1, 2, 11, 13, 1, 11, 11, 14, 1, 9, 14, 3, 7, 14, 30, 60, 90]

    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:
            mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return mcmr
    

def process_pcce(pcce):
    op = pcce["operational_progresses"].copy()
    if op.empty:
        return None
    key = "achievements"


    pcce[key] = pd.DataFrame()
    start_date = pcce["disasters"][pcce["disasters"].columns[1]].apply(convert_date)

    surge_date = pcce["disasters"][pcce["disasters"].columns[1]].copy()
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    pcce[key]["Ref"] = pcce["disasters"].index
    pcce[key].set_index("Ref", inplace=True)
    deltas = [3, 3, 4, 11, 18, 7, 2, 3, 11, 13, 2, 7, 1, 11, 11, 14, 1, 3, 7, 14, 30, 11, 14, 16, 20, 32, 28, 7, 7, 6, 60, 90]
    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            pcce[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:   
            pcce[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return pcce


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def generate_overview(bucket, sheets):
    ea = process_ea(bucket["EA"])
    dref = process_dref(bucket["DREF"])
    mcmr = process_mcmr(bucket["MCMR"])
    pcce = process_pcce(bucket["PCCE"])
    dref_escalated = process_dref_escalated(bucket["DREF_ESCALATED"])

    key = "achievements"
    split_dict = {}
    for sheet_name, sheet in sheets.items():
        if "EA" in sheet_name and ea != None:
            split_dict["EA"] = area_split_ea(ea[key], sheet.columns.tolist(), bucket["EA"]["disasters"])
        elif "DREF_ESCALATED" in sheet_name and dref_escalated != None:
            split_dict["DREF_ESCALATED"] = area_split_dref_escalated(dref_escalated[key], sheet.columns.tolist(), bucket["DREF_ESCALATED"]["disasters"])
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
    cols = ["Ref", "Area", "Achieved", "Not Achieved", "Upcoming", "Achieved Early", "Achieved Late", "N/A", "Data Completeness", "General Performance"]
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
    df4 = read_area_info_folder(area_split_dfs.get("DREF_ESCALATED", {}))
    df_combined = pd.concat([df, df1, df2, df3])
    
    return df_combined

'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

def calculate_delta(today, status, expected, d):
    if pd.notna(status) and status == "N/A":
        delta = "N/A"
    else:
        if d == 90:
            if status == "Upcoming":
                delta = expected - today
                delta = f"{delta.days} days to deadline"
            else:
                delta = today - expected
                delta = f"{delta.days} days behind deadline"
        else:
            delta = d * -1
            if delta == 0:
                delta = "Achieved On Date"
            elif delta > 0:
                delta = f"Achieved {delta} days early"
            else:
                delta = f"Achieved {delta * -1} days late"
    return delta


def read_task_info(df, op, op_df, area):
    cols = df.columns[9:].copy()
    task_infos = []
    today = pd.Timestamp(date.today())
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
            status = row[a] if pd.notna(row[a]) else "Not Achieved"
            if row[b] > 90:
                raw_delta = 90
            else:
                raw_delta = row[b]
            delta = calculate_delta(today, row[a], row[d], raw_delta)
            task_infos.append({
                "Ref" : row["Ref"],
                "EWTS Varient" : op,
                "Area" : area,
                "Task" : e,
                "Status" : status,
                "Completed" : str(row[c])[:10],
                "Expected Date" : "-" if row[a] == "N/A" else str(row[d])[:10],
                "Delta" : delta,
                "Raw_Delta": raw_delta,
                "Escalated" : escalated,
            })
    return task_infos


def read_im(df, op, op_df):
    cols = df.columns[9:].copy()
    task_infos = []
    today = pd.Timestamp(date.today())
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
            status = row[a] if pd.notna(row[a]) else "Not Achieved"
            if row[b] > 90:
                raw_delta = 90
            else:
                raw_delta = row[b]
            delta = calculate_delta(today, row[a], row[d], raw_delta)
            task_infos.append({
                "Ref" : row["Ref"],
                "EWTS Varient" : op,
                "Area" : "Information Management",
                "Task" : a.replace("_", " "),
                "Status" : status,
                "Completed" : str(row[c])[:10],
                "Expected Date": "-" if row[a] == "N/A" else str(row[d])[:10],
                "Delta" : delta,
                "Raw_Delta": raw_delta,
                "Escalated" : escalated,
            })
    return task_infos


status_mapping = {
    "Achieved" : 3,
    "Achieved Early" : 4,
    "Achieved Late" : 2,
    "N/A" : 0,
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
    task_infos = areas_in_op(area_split_dfs.get("EA", {}), "ea") + areas_in_op(area_split_dfs.get("DREF", {}), "dref") + areas_in_op(area_split_dfs.get("MCMR", {}), "mcmr") + areas_in_op(area_split_dfs.get("PCCE", {}), "protracted crisis") + areas_in_op(area_split_dfs.get("DREF_ESCALATED", {}), "dref_escalated")
    df = pd.DataFrame(task_infos)
    df["Score"] = df["Status"].map(status_mapping)
    df["Delta"] = df["Delta"].astype(str)
    return df



sheets = {}
sheets["EA"] = spark.read.table("ea").toPandas()
sheets["DREF"] = spark.read.table("dref").toPandas()
dref_cols = sheets["DREF"].columns
sheets["MCMR"] = spark.read.table("mcmr").toPandas()
sheets["Protracted"] = spark.read.table("pcce").toPandas()
sheets["DREF_ESCALATED"] = spark.read.table("dref_2").toPandas()
escalation = spark.read.table("escalation_events").toPandas()
surge_requests = spark.read.table("SURGE").toPandas()
surge_requests = surge_requests.set_index("Ref")
bucket = organize_sheets()
area_split_dfs = generate_overview(bucket, sheets)
general_df = pd.concat([i["General Information"] for _, i in area_split_dfs.items()])
general_df.reset_index(inplace=True)
general_df = general_df[['Ref'] + [col for col in general_df.columns[:20] if col != 'Ref']]
general_df["Launch Date"] = general_df["Launch Date"].apply(convert_date)
spark_general_info = spark.createDataFrame(general_df)
spark_general_info = spark_general_info.toDF(*[c.replace(" ", "_") for c in spark_general_info.columns])
spark_general_info = spark_general_info.withColumn("Trigger_Date", to_date("Trigger_Date", "M/d/yyyy"))
spark_general_info = spark_general_info.withColumn("Launch_Date", to_date("Launch_Date", "M/d/yyyy"))
spark_general_info.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.general_info")

df_area_info = area_info(area_split_dfs)

ti = task_info_extraction(area_split_dfs)

spark_task_infos = spark.createDataFrame(ti)
spark_task_infos = spark_task_infos.toDF(*[c.replace(" ", "_") for c in spark_task_infos.columns])
spark_task_infos = spark_task_infos.withColumn(
    "Score",
    when(((col("Status") == "N/A") | (col("Status") == "Upcoming")) & (col("Score") == 0), None)
    .otherwise(col("Score"))
)
spark_task_infos.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.task_info")
df_area_info["General Performance"] = df_area_info["General Performance"].fillna(0)
df_area_info.loc[(df_area_info["Achieved"] == 0) & (df_area_info["Achieved Early"] == 0) & (df_area_info["Achieved Late"] == 0) & (df_area_info["Not Achieved"] == 0), "General Performance"] = None
# print(df_area_info.columns)
spark_area_info = spark.createDataFrame(df_area_info)
spark_area_info = spark_area_info.toDF(*[c.replace(" ", "_") for c in spark_area_info.columns])

spark_area_info.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.area_info")