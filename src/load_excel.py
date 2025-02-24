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
    sheet["Ref"] = "EA" + sheet[col_name[3]] + sheet[col_name[5]] 
    disasters = sheet.loc[:, col_name[:10]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[10:14] + col_name[26:30] + col_name[43:49] + col_name[73:]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[56:61]]
    dashboard_progress = sheet.loc[:, [col_name[0]] + col_name[51:56]]
    sec_coverage = sheet.loc[:, [col_name[0]] + col_name[14:20]]
    fed_coverage = sheet.loc[:, [col_name[0]] + col_name[20:26]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[30:43]]
    nfi = sheet.loc[:, [col_name[0]] + col_name[49:51] + col_name[61:64]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[64:73]]
    disasters.to_csv(f"{root}disasters.csv")
    operational_progresses.to_csv(f"{root}operational_progresses.csv")
    financial_progress.to_csv(f"{root}financial_progress.csv")
    dashboard_progress.to_csv(f"{root}dashboard_progress.csv")
    sec_coverage.to_csv(f"{root}sec_coverage.csv")
    fed_coverage.to_csv(f"{root}fed_coverage.csv")
    rrp.to_csv(f"{root}rrp.csv")
    nfi.to_csv(f"{root}nfi.csv")
    operational_achievements.to_csv(f"{root}operational_achievements.csv")
    print("EA master data organized")


def organize_dref(sheet):
    return "hi"


def organize_mcmr(sheet):
    return "hi"


def organize_protracted(sheet):
    return "hi"

def organize_sheets(sheets):
    for sheet_name, sheet in sheets.items():
        if "DREF" in sheet_name:
            organize_dref(sheet)
        elif "EA" in sheet_name:
            organize_ea(sheet)
        elif "MCMR" in sheet_name:
            organize_mcmr(sheet)
        elif "Protracted" in sheet_name:
            organize_protracted(sheet)
        else:
            print("Sheet name not recognized")
    print("organization complete")

def main():
    filename = "../dummy_data/ewts_master_dummy_data.xlsx"
    sheet_dict = load_excel(filename)
    organize_sheets(sheet_dict)

if __name__ == "__main__":
    main()