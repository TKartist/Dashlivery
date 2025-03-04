import pandas as pd
import sys


def load_excel(path):
    
    try:    
        xls = pd.ExcelFile(path)
        
        sheets = {sheet_name : xls.parse(sheet_name) for sheet_name in xls.sheet_names}
        
        for sheet in sheets.values():
            sheet.dropna(subset=[sheet.columns[1]], inplace=True)
        return sheets
    except Exception as e:
        print(f"error while loading {path} for excel data: {e}")
        sys.exit(1)



def organize_ea(sheet):
    root = "../organized_ea/"
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "EA" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:15] + col_name[27:31] + col_name[44:50] + col_name[74:]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[57:62]]
    dashboard_progress = sheet.loc[:, [col_name[0]] + col_name[52:57]]
    sec_coverage = sheet.loc[:, [col_name[0]] + col_name[15:21]]
    fed_coverage = sheet.loc[:, [col_name[0]] + col_name[21:27]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[31:44]]
    nfi = sheet.loc[:, [col_name[0]] + col_name[50:52] + col_name[62:65]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[65:74]]
    
    sec_coverage = sec_coverage.replace('\n', ' ', regex=True)
    sec_coverage = sec_coverage.replace(' ', ' ', regex=True)
    

    print("EA master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref"), "financial_progress" : financial_progress.set_index("Ref"), "dashboard_progress" : dashboard_progress.set_index("Ref"), "sec_coverage" : sec_coverage.set_index("Ref"), "fed_coverage" : fed_coverage.set_index("Ref"), "rrp" : rrp.set_index("Ref"), "nfi" : nfi.set_index("Ref"), "operational_achievements" : operational_achievements.set_index("Ref")}

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
    root = "../organized_mcmr/"
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "MCMR" + sheet[col_name[4]] + sheet[col_name[6]] 
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
    root = "../organized_pcce/"
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

def organize_sheets(sheets):
    bucket = {}
    for sheet_name, sheet in sheets.items():
        if "DREF" in sheet_name:
            bucket["DREF"] = organize_dref(sheet)
        elif "EA" in sheet_name:
            bucket["EA"] = organize_ea(sheet)
        elif "MCMR" in sheet_name:
            bucket["MCMR"] = organize_mcmr(sheet)
        elif "Protracted" in sheet_name:
            bucket["Protracted"] = organize_protracted(sheet)
        else:
            print("Sheet name not recognized")
    print("organization complete")
    return bucket