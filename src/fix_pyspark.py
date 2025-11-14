def organize_ea(sheet):
    # Take the EA sheet, build a unique reference ID for each row,
    # then split it into 3 smaller tables: disasters, operational progress, and DREF shift info.
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "EA" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11] + [col_name[75]]]
    ops_details = sheet.loc[:, [col_name[0]] + col_name[11:16] + col_name[22:26] + col_name[38:58] + col_name[69:70] + col_name[71:75] + col_name[76:81]]

    dref_shift = sheet.loc[:, [col_name[0]] + [col_name[70]]]
    print("EA master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : ops_details.set_index("Ref"), "dref_shift" : dref_shift.set_index("Ref")}

def organize_dref(sheet):
    # Same idea as organize_ea but for DREF data: add reference ID, and split into disasters and operational progress tables.
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11] + [col_name[52]]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:19] + col_name[32:39] + col_name[51:52] + col_name[53:56]]

    print("DREF master data organized")
    return {"disasters": disasters.set_index("Ref"), "operational_progresses": operational_progresses.set_index("Ref")}

def organize_dref_escalated(sheet):
    # Organize DREF escalated sheet: set reference ID, tag it as "DREF 2nd Allocation", and split into disasters + progress.
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[4]] + sheet[col_name[6]]
    sheet["EWTS Varient_"] = "DREF 2nd Allocation"
    disasters = sheet.loc[:, col_name[:11] + [col_name[49]]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:17] + col_name[30:37]]
    print("DREF master data organized")
    return {"disasters": disasters.set_index("Ref"), "operational_progresses": operational_progresses.set_index("Ref")}

def organize_mcmr(sheet):
    # Organize multi-country (MCMR) sheet: create ref ID and split into disasters + operational progress tables.
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "MCMR" + sheet[col_name[6]] 
    disasters = sheet.loc[:, col_name[:11] + [col_name[53]]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:14] + col_name[20:23] + col_name[36:45] + col_name[47:53]]

    print("MCMR master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref")}


def organize_protracted(sheet):
    # Organize protracted crisis (PCCE) sheet: create ref ID and split into disasters + operational progress tables.
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "PCCE" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:15] + col_name[21:25] + col_name[39:57] + col_name[57:58] + col_name[60:65]]

    print("PCCE master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref")}

def organize_sheets():
    # Read the different Spark tables into pandas, organize each type,
    # and return everything in one big dictionary called bucket.
    bucket = {}
    bucket["DREF"] = organize_dref(spark.read.table("dref").toPandas())
    bucket["EA"] = organize_ea(spark.read.table("ea").toPandas())
    bucket["MCMR"] = organize_mcmr(spark.read.table("mcmr").toPandas())
    bucket["PCCE"] = organize_protracted(spark.read.table("pcce").toPandas())
    bucket["DREF_ESCALATED"] = organize_dref_escalated(spark.read.table("dref_two").toPandas())
    return bucket


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def full_list(cols):
    # For each indicator column, build the related set of columns:
    # [status, days difference, actual date, expected date].
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
    # If a row is classified as 'Orange', set 'Not Achieved' cells to 'N/A' (ignore them),
    # otherwise leave the row as-is.
    idx = row.name
    if df_classification[idx] == 'Orange':
        return row.apply(lambda x: 'N/A' if x == 'Not Achieved' else x)
    return row


def correct_im(df, df_classification):
    # Apply replace_category to all rows in the Information Management (IM) dataframe.
    df = df.copy()
    df = df.apply(lambda row: replace_category(row, df_classification), axis=1)

    return df


def summarize_df(df):
    # For each row, count how many indicators fall into each category
    # and compute "Data Completeness" and "General Performance" scores.
    df = df.copy()
    categories = ["Achieved", "Not Achieved", "Achieved Early", "Achieved Late", "N/A", "Upcoming"]

    # Count how many times each status appears per row
    for category in categories:
        df.loc[:, category] = df.apply(lambda x: sum(str(cell) == category for cell in x), axis=1)
    
    # Data completeness = how many achieved vs total that could be achieved
    dc_num = df["Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    dc_den = df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    df["Data Completeness"] = np.where(dc_den != 0, dc_num / dc_den, 1)

    # General performance = weighted score of outcome types
    numerator = df["Achieved"] * 3 + df["Achieved Early"] * 4 + df["Achieved Late"] * 2
    denominator = (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 4
    df["General Performance"] = np.where(denominator != 0, numerator / denominator, 0)

    # Move summary columns to the front
    cols_to_move = ["Achieved", "Not Achieved", "Upcoming", "Achieved Early", "Achieved Late", "N/A", "Data Completeness", "General Performance"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]
    return df


def update_general_info(folder, general):
    # Combine summary stats from all areas (like Assessment, Logistics...) into one "general" overview table.
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
    
    # Recompute completeness and performance at the general level
    dc_num = df["Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    dc_den = df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]
    df["Data Completeness"] = np.where(dc_den != 0, dc_num / dc_den, 1)
    
    numerator = df["Achieved"] * 3 + df["Achieved Early"] * 4 + df["Achieved Late"] * 2
    denominator = (df["Achieved"] + df["Not Achieved"] + df["Achieved Early"] + df["Achieved Late"]) * 4
    df["General Performance"] = np.where(denominator != 0, numerator / denominator, 0)

    return df



'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def area_split_ea(overview, columns, general):
    # For EA data, slice the big "achievements" table into thematic areas
    # (Assessment, Planning, Surge, HR, etc.), summarize each, and add General Information.
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:16] + [columns[22]] + columns[79:81])] # add EA coverage
    surge = overview[full_list(columns[23:26])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[38:40])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[40:44])] # Upcoming joint statement in master data
    logistics = overview[full_list(columns[44:47])]
    # im = overview[full_list(columns[47:52] + columns[71:73])]
    im = overview[full_list(columns[47:51])]
    risk = overview[full_list(columns[73:75])]
    finance = overview[full_list(columns[52:56] + columns[77:79])]
    program_delivery = overview[full_list(columns[56:58])]
    security = overview[full_list(columns[69:70] + columns[76:77])]

    areas = {}
    areas["Assessment"] = summarize_df(assessment)
    areas["Planning"] = summarize_df(resource_mobilization)
    areas["Risk"] = summarize_df(risk)
    areas["Surge"] = summarize_df(surge)
    areas["HR"] = summarize_df(hr)
    areas["Coordination"] = summarize_df(coordination)
    areas["Logistics"] = summarize_df(logistics)
    areas["Information Management"] = summarize_df(correct_im(im, general["Classification_"]))
    areas["Finance"] = summarize_df(finance)
    areas["Security"] = summarize_df(security)
    areas["Program Delivery"] = summarize_df(program_delivery)
    general_info = update_general_info(areas, general)
    areas["General Information"] = general_info
    return areas

def area_split_dref_escalated(overview, columns, general):
    # For escalated DREF data, split indicators into areas and summarize them.
    assessment = overview[full_list(columns[11])]
    resource_mobilization = overview[full_list(columns[12:14])]
    surge = overview[full_list(columns[14:17])]
    logistics = overview[full_list(columns[30:32])]
    finance = overview[full_list(columns[32:36])]
    delivery = overview[full_list([columns[36]])]

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
    # For regular DREF data, split the "achievements" table into areas and compute summaries.
    assessment = overview[full_list(columns[11])]
    risk = overview[full_list(columns[12:14])]
    resource_mobilization = overview[full_list(columns[14:16])]
    surge = overview[full_list(columns[16:19])]
    logistics = overview[full_list(columns[32:34])]
    finance = overview[full_list(columns[34:38] + columns[54:56])]
    delivery = overview[full_list([columns[38]])] # add targeted population, ehi distribution, and implementation rate
    security = overview[full_list(columns[51:52] + columns[53:54])]

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
    # For multi-country (MCMR), split achievements into areas and summarize each.
    resource_mobilization = overview[full_list(columns[11:14] + [columns[20]])] # add coverage
    surge = overview[full_list(columns[21:23])] # add % related values to the surge (rrp)
    hr = overview[full_list(columns[36:38])] # add % related values to the hr (rrp)
    coordination = overview[full_list(columns[38])]
    logistics = overview[full_list(columns[39:42])]
    # im = overview[full_list(columns[42:43] + columns[47:53])]
    im = overview[full_list(columns[42:43] + columns[47:50])]
    finance = overview[full_list(columns[43:45])]

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
    # For protracted crisis (PCCE), split achievements into areas and summarize each.
    assessment = overview[columns[11:12]]
    resource_mobilization = overview[full_list(columns[12:15] + [columns[21]] + [columns[62]])]
    surge = overview[full_list(columns[22:25])]
    hr = overview[full_list(columns[39:41])]
    coordination = overview[full_list(columns[41:44])]
    logistics = overview[full_list(columns[44:47])]
    # im = overview[full_list(columns[47:52] + columns[63:65])]
    im = overview[full_list(columns[47:51])]

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
    # Try to turn a messy date string into a proper datetime object.
    # If it's a known placeholder like "-", "DNU", "Not Achieved", or "N/A", just return it as-is.
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
    # For EA indicators, decide if a task is:
    # "Upcoming", "Not Achieved", "N/A", "Achieved Early", "Achieved", or "Achieved Late"
    # based on start date, completion date, and deadline rules.
    keys = row.index.tolist()
    r0, r1 = row.iloc[0], row.iloc[1]  # r0 = start date, r1 = completion or status
    # If the third value in the row is "Yes", use the second limit (l2), else use first limit (l1)
    if row.iloc[2] == "Yes":
        limit = l2
    else:
        limit = l1
    expected_date = r0 + pd.Timedelta(days=limit)
    if r1 == "-" or pd.isna(r1):
        # No completion date: check if deadline passed or not
        deadline = r0 + pd.Timedelta(days=limit + 1)
        if deadline >= datetime.now():
            return pd.Series(["Upcoming", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"]) 
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    if r1 == "DNU" or r1 == "N/A":
        # Explicitly marked as not applicable
        return pd.Series(["N/A", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    if r1 == "Not Achieved":
        # Explicitly marked as not achieved
        return pd.Series(["Not Achieved", 365, "-", expected_date], index=[keys[1], f"{keys[1]} (days)", f"{keys[1]} date", f"{keys[1]} expected date"])
    
    # Small tweak to treat 30 days as 31, probably because of month-length edge cases
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
    # Same as determine_status_ea but for non-EA logic (single deadline limit).
    keys = row.index.tolist()
    r0, r1 = row.iloc[0], row.iloc[1]
    expected_date = r0 + pd.Timedelta(days=limit)
    if r1 == "-" or pd.isna(r1):
        deadline = r0 + pd.Timedelta(days=limit + 1)
        if deadline >= datetime.now():
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
    # Take organized EA data (disasters + operational progress) and compute detailed
    # achievement status columns for each indicator with deadlines.
    op = ea["operational_progresses"].copy()
    if op.empty:
        return None
    key = "achievements"

    ea[key] = pd.DataFrame()
    if "Trigger Date_" in ea["disasters"].columns:
        ea["disasters"] = ea["disasters"].rename(columns={"Trigger Date_":"Trigger_Date_"})
    start_date = ea["disasters"]["Trigger_Date_"].apply(convert_date)
    # Hard-coded correction for one specific reference
    start_date["EANigeriaMDRNG042"] = ea["disasters"]["Launch Date"]["EANigeriaMDRNG042"]
    launch_date = ea["disasters"]["Launch Date"].apply(convert_date)
    surge_date = ea["disasters"]["Trigger_Date_"].copy()
    # Override surge dates with SURGE table where available
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    ea[key]["Ref"] = ea["disasters"].index
    ea[key].set_index("Ref", inplace=True)
    # Predefined index positions that behave differently for deadlines
    points = [4, 5, 9, 10, 15, 16, 23, 24, 25, 26, 27, 28, 32, 33]
    # Deltas define how many days each indicator has to be completed
    deltas = [3, 3, 0, 4, 11, 18, 1, 2, 3, 11, 13, 2, 4, 7, 1, 11, 11, 14, 1, 3, 7, 14, 30, 11, 14, 16, 20, 32, 18, 7, 60, 90, 18, 34, 7, 2, 4, 60, 7]
    deltas_b = [3, 3, 0, 0, 7, 14, 1, 2, 3, 7, 9, 2, 4, 7, 1, 7, 7, 10, 1, 3, 7, 14, 30, 7, 10, 12, 16, 29, 14, 7, 60, 90, 14, 30, 7, 2, 4, 60, 3]  
    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            # Some surge tasks are counted from launch date, others from surge date
            if i in points:
                ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.concat([launch_date, op[on[i]], ea["dref_shift"]], axis=1).apply(determine_status_ea, args=(deltas[i] - 4,deltas_b[i] - 4,), axis=1)
            else:
                ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.concat([surge_date, op[on[i]], ea["dref_shift"]], axis=1).apply(determine_status_ea, args=(deltas[i],deltas_b[i],), axis=1)
        else:
            # Non-surge tasks start counting from launch or trigger date
            if i in points:
                ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.concat([launch_date, op[on[i]], ea["dref_shift"]], axis=1).apply(determine_status_ea, args=(deltas[i] - 4,deltas_b[i] - 4,), axis=1)
            else:
                ea[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.concat([start_date, op[on[i]], ea["dref_shift"]], axis=1).apply(determine_status_ea, args=(deltas[i],deltas_b[i],), axis=1)
    return ea

def process_dref(dref):
    # Same idea as process_ea but for DREF data, using its own rules and deadlines.
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
    launch_date = dref["disasters"]["Launch Date"].apply(convert_date)
    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    
    dref[key]["Ref"] = dref["disasters"].index
    dref[key].set_index("Ref", inplace=True)
    
    points = [1, 8, 9, 10, 11, 12, 13, 14]
    deltas = [3, 12, 14, 14, 14, 1, 2, 3, 19, 22, 17, 21, 22, 30, 30, 7, 7, 2, 4]
    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            if i in points:
                dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i] - 4,), axis=1)
            else:
                dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:
            if i in points:
                dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i] - 4,), axis=1)
            else:
                dref[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return dref

def process_dref_escalated(dref_escalated):
    # Process escalated DREF data, computing achievement status per indicator using its deadline setup.
    op = dref_escalated["operational_progresses"].copy()
    
    if op.empty:
        return None
    
    key = "achievements"
    dref_escalated[key] = pd.DataFrame()
    start_date = dref_escalated["disasters"]["Trigger Date_"].apply(convert_date)
    surge_date = dref_escalated["disasters"]["Trigger Date_"].copy()
    for ref, val in surge_requests.iterrows():
        surge_date[ref] = val["requested-on"]
    launch_date = dref_escalated["disasters"]["Launch Date"].apply(convert_date)
    surge_date = surge_date.apply(convert_date)

    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    
    dref_escalated[key]["Ref"] = dref_escalated["disasters"].index
    dref_escalated[key].set_index("Ref", inplace=True)

    deltas = [3, 10, 10, 1, 2, 3, 11, 14, 14, 6, 7, 10, 34]
    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            if i > 6:
                dref_escalated[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i] - 4,), axis=1)
            else:
                dref_escalated[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:
            if i > 6:
                dref_escalated[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i] - 4,), axis=1)
            else:    
                dref_escalated[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return dref_escalated

def process_mcmr(mcmr):
    # Process multi-country (MCMR) data: compute status for each operational indicator.
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
    launch_date = mcmr["disasters"]["Launch Date"].apply(convert_date)
    on = op.columns
    for col in on:
        op[col] = op[col].apply(convert_date)
    
    mcmr[key]["Ref"] = mcmr["disasters"].index
    mcmr[key].set_index("Ref", inplace=True)
    points = [2, 3, 6, 7, 9, 10, 11, 13, 14]
    deltas = [3, 4, 11, 18, 1, 2, 11, 13, 1, 11, 11, 14, 1, 9, 14, 3, 7, 14, 30, 60, 90]

    for i in range(len(deltas)):
        if "Surge" in on[i] or "RR" in on[i]:
            if i in points:
                mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i] - 4,), axis=1)
            else:
                mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:
            if i in points:
                mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i] - 4,), axis=1)
            else:
                mcmr[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
    return mcmr
    

def process_pcce(pcce):
    # Process protracted crisis (PCCE) data: compute deadlines and achievement status per indicator.
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
            if i > 5:
                pcce[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(launch_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
            else:
                pcce[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(surge_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)
        else:   
            pcce[key][[on[i], f"{on[i]} (days)", f"{on[i]} date", f"{on[i]} expected date"]] = pd.merge(start_date, op[on[i]], left_index=True, right_index=True).apply(determine_status, args=(deltas[i],), axis=1)

    return pcce


'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''


def generate_overview(bucket, sheets):
    # Run the processing functions for each operation type (EA, DREF, etc.),
    # then split achievements into areas and return all the area-level data.
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
    # Flatten all area-level tables for a single operation type into one dataframe
    # with standard columns (Ref, Area, Achieved, Not Achieved, etc.).
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
    # Combine area info for all operation types (EA, DREF, MCMR, PCCE, DREF_ESCALATED) into a single dataframe.
    df = read_area_info_folder(area_split_dfs.get("EA", {}))
    df1 = read_area_info_folder(area_split_dfs.get("DREF", {}))
    df2 = read_area_info_folder(area_split_dfs.get("MCMR", {}))
    df3 = read_area_info_folder(area_split_dfs.get("PCCE", {}))
    df4 = read_area_info_folder(area_split_dfs.get("DREF_ESCALATED", {}))
    df_combined = pd.concat([df, df1, df2, df3])
    
    return df_combined

'''----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

def calculate_delta(today, status, expected, d):
    # Turn raw numeric or date-based lag info into human-readable text like
    # "5 days to deadline", "3 days behind deadline", or "Achieved 2 days early".
    if pd.notna(status) and status == "N/A":
        delta = "N/A"
    else:
        if d == 90:
            # For "Upcoming" style entries, use expected date vs today
            if status == "Upcoming":
                delta = expected - today
                if delta.days == 0:
                    delta = f"Due Today"
                else:
                    delta = f"{delta.days} days to deadline"
            else:
                delta = today - expected
                delta = f"{delta.days} days behind deadline"
        else:
            # For completed tasks, d is a signed number of days early/late
            delta = d * -1
            if delta == 0:
                delta = "Achieved On Date"
            elif delta > 0:
                delta = f"Achieved {delta} days early"
            else:
                delta = f"Achieved {delta * -1} days late"
    return delta


def read_task_info(df, op, op_df, area):
    # From an area-level summary dataframe, build a row-per-task structure with status, dates,
    # delta text, escalation info, etc., for one operation type.
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
    # Same as read_task_info but specifically for the Information Management area,
    # which uses a fixed indicator text to look up escalation.
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
    # Map a status label to a numeric score for later aggregation.
    "Achieved" : 75,
    "Achieved Early" : 100,
    "Achieved Late" : 50,
    "N/A" : 0,
    "Upcoming" : 0,
    "Not Achieved" : 0,
}


def areas_in_op(adf, op):
    # For a single operation type (e.g. EA or DREF),
    # loop through all its areas and build a list of task info dictionaries.
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
    # Gather task-level info for all operation types into a single dataframe
    # and attach a numeric score for each task based on its status.
    task_infos = areas_in_op(area_split_dfs.get("EA", {}), "ea") + areas_in_op(area_split_dfs.get("DREF", {}), "dref") + areas_in_op(area_split_dfs.get("MCMR", {}), "mcmr") + areas_in_op(area_split_dfs.get("PCCE", {}), "protracted crisis") + areas_in_op(area_split_dfs.get("DREF_ESCALATED", {}), "dref_escalated")
    df = pd.DataFrame(task_infos)
    df["Score"] = df["Status"].map(status_mapping)
    df["Delta"] = df["Delta"].astype(str)
    return df



# ==== MAIN FLOW ====
# From here down, we're not defining functions anymore.
# This is the "orchestrator" that:
# 1) pulls raw data from Spark
# 2) organizes + processes it
# 3) calculates summaries and scores
# 4) writes the final cleaned tables back to Spark


# 1) READ RAW TABLES FROM SPARK INTO PANDAS
sheets = {}
sheets["EA"] = spark.read.table("ea").toPandas()
sheets["DREF"] = spark.read.table("dref").toPandas()
dref_cols = sheets["DREF"].columns
sheets["MCMR"] = spark.read.table("mcmr").toPandas()
sheets["Protracted"] = spark.read.table("pcce").toPandas()
sheets["DREF_ESCALATED"] = spark.read.table("dref_two").toPandas()
escalation = spark.read.table("escalation_events").toPandas()
surge_requests = spark.read.table("SURGE").toPandas()
surge_requests = surge_requests.set_index("Ref")  # make "Ref" the key so we can look up surge dates fast

# 2) ORGANIZE RAW SHEETS INTO STRUCTURED BUCKET (DISASTERS + OP PROGRESS + SHIFT INFO)
bucket = organize_sheets()

# 3) PROCESS EACH OPERATION TYPE TO BUILD "ACHIEVEMENTS" AND SPLIT THEM BY AREA
area_split_dfs = generate_overview(bucket, sheets)

# 4) BUILD GENERAL-LEVEL OVERVIEW TABLE ACROSS ALL OPS
#    - Take "General Information" from every operation type
#    - Stack them together into one big dataframe
general_df = pd.concat([i["General Information"] for _, i in area_split_dfs.items()])
general_df.reset_index(inplace=True)

# Make sure "Ref" is the first column, then keep first 20 columns
general_df = general_df[['Ref'] + [col for col in general_df.columns[:20] if col != 'Ref']]

# Normalize launch date to proper datetime
general_df["Launch Date"] = general_df["Launch Date"].apply(convert_date)

# 5) PUSH GENERAL INFO BACK TO SPARK AS A CLEAN TABLE
spark_general_info = spark.createDataFrame(general_df)
# Replace spaces with underscores in column names to avoid Spark whining
spark_general_info = spark_general_info.toDF(*[c.replace(" ", "_") for c in spark_general_info.columns])
# Make sure date columns are actual date type in Spark
spark_general_info = spark_general_info.withColumn("Trigger_Date", to_date("Trigger_Date", "M/d/yyyy"))
spark_general_info = spark_general_info.withColumn("Launch_Date", to_date("Launch_Date", "M/d/yyyy"))
# Overwrite the target Delta table with the new data
spark_general_info.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.general_info")

# 6) BUILD AREA-LEVEL SUMMARY TABLE (PER AREA, PER OPERATION)
df_area_info = area_info(area_split_dfs)

# 7) EXTRACT TASK-LEVEL INFO (ONE ROW PER TASK) FROM THE AREA SPLITS
ti = task_info_extraction(area_split_dfs)

# Clean trailing spaces from task names
ti["Task"] = ti["Task"].str.rstrip()

# Drop one very specific, noisy task row that you don't want in the output
ti = ti[~ti["Task"].str.contains("Working Advance Request  IRP Form  signed by IFRC and NS no longer than 2 days from the DREF approval", na=False)]

# 8) PUSH TASK-LEVEL TABLE TO SPARK
spark_task_infos = spark.createDataFrame(ti)
spark_task_infos = spark_task_infos.toDF(*[c.replace(" ", "_") for c in spark_task_infos.columns])

# For N/A or Upcoming tasks with score=0, remove the score (set to null) so they don't drag averages
spark_task_infos = spark_task_infos.withColumn(
    "Score",
    when(((col("Status") == "N/A") | (col("Status") == "Upcoming")) & (col("Score") == 0), None)
    .otherwise(col("Score"))
)
spark_task_infos.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.task_info")

# 9) FINAL CLEANUP FOR AREA-LEVEL INFO BEFORE SENDING TO SPARK
# Fill missing performance with 0 first
df_area_info["General Performance"] = df_area_info["General Performance"].fillna(0)

# If literally nothing was done in that area (all zeros), set performance back to None
df_area_info.loc[
    (df_area_info["Achieved"] == 0) &
    (df_area_info["Achieved Early"] == 0) &
    (df_area_info["Achieved Late"] == 0) &
    (df_area_info["Not Achieved"] == 0),
    "General Performance"
] = None

# 10) PUSH AREA-LEVEL SUMMARY TABLE TO SPARK
spark_area_info = spark.createDataFrame(df_area_info)
spark_area_info = spark_area_info.toDF(*[c.replace(" ", "_") for c in spark_area_info.columns])

spark_area_info.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("master_data_processing.area_info")
