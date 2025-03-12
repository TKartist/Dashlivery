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
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "EA" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[11:16] + col_name[29:33] + col_name[46:53] + col_name[77:]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[60:65]]
    dashboard_progress = sheet.loc[:, [col_name[0]] + col_name[55:60]]
    sec_coverage = sheet.loc[:, [col_name[0]] + col_name[16:22]]
    fed_coverage = sheet.loc[:, [col_name[0]] + col_name[22:28]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[33:46]]
    nfi = sheet.loc[:, [col_name[0]] + col_name[53:55] + col_name[65:68]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[68:77]]
    
    sec_coverage = sec_coverage.replace('\n', ' ', regex=True)
    sec_coverage = sec_coverage.replace(' ', ' ', regex=True)
    disasters.to_csv("../organized_ea/general_info.csv", index=False)

    print("EA master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref"), "financial_progress" : financial_progress.set_index("Ref"), "dashboard_progress" : dashboard_progress.set_index("Ref"), "sec_coverage" : sec_coverage.set_index("Ref"), "fed_coverage" : fed_coverage.set_index("Ref"), "rrp" : rrp.set_index("Ref"), "nfi" : nfi.set_index("Ref"), "operational_achievements" : operational_achievements.set_index("Ref")}

def organize_dref(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[12:20] + col_name[33:35] + col_name[52:54]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[20:33]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[35:42]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[42:52]]
    disasters.to_csv("../organized_dref/general_info.csv", index=False)

    print("DREF master data organized")
    return {"disasters": disasters.set_index("Ref"), "operational_progresses": operational_progresses.set_index("Ref"), "rrp": rrp.set_index("Ref"), "financial_progress": financial_progress.set_index("Ref"), "operational_achievements": operational_achievements.set_index("Ref")}

def organize_mcmr(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "MCMR" + sheet[col_name[4]] + sheet[col_name[6]] 
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[12:15] + col_name[21:23] + col_name[36:43]]
    total_coverage = sheet.loc[:, [col_name[0]] + col_name[15:21]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[23:36]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[43:45]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[45:]]

    total_coverage = total_coverage.replace('\n', ' ', regex=True)
    total_coverage = total_coverage.replace(' ', ' ', regex=True)
    disasters.to_csv("../organized_mcmr/general_info.csv", index=False)

    print("MCMR master data organized")
    return {"disasters" : disasters.set_index("Ref"), "operational_progresses" : operational_progresses.set_index("Ref"), "total_coverage" : total_coverage.set_index("Ref"), "rrp" : rrp.set_index("Ref"), "financial_progress" : financial_progress.set_index("Ref"), "operational_achievements" : operational_achievements.set_index("Ref")}


def organize_protracted(sheet):
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "PCCE" + sheet[col_name[4]] + sheet[col_name[6]]
    disasters = sheet.loc[:, col_name[:11]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[12:16] + col_name[22:26] + col_name[40:48] + col_name[58:59] + col_name[61:63]]
    coverage = sheet.loc[:, [col_name[0]] + col_name[16:22]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[26:40]]
    dashboard = sheet.loc[:, [col_name[0]] + col_name[48:53]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[53:58]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[59:61]]

    coverage = coverage.replace('\n', ' ', regex=True)
    coverage = coverage.replace(' ', ' ', regex=True)
    disasters.to_csv("../organized_pcce/general_info.csv", index=False)
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