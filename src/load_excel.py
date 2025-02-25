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
    
    sec_coverage = sec_coverage.replace('\n', ' ', regex=True)
    sec_coverage = sec_coverage.replace(' ', ' ', regex=True)
    
    disasters.to_csv(f"{root}disasters.csv", index=False)
    operational_progresses.to_csv(f"{root}operational_progresses.csv", index=False)
    financial_progress.to_csv(f"{root}financial_progress.csv", index=False)
    dashboard_progress.to_csv(f"{root}dashboard_progress.csv", index=False)
    sec_coverage.to_csv(f"{root}sec_coverage.csv", index=False)
    fed_coverage.to_csv(f"{root}fed_coverage.csv", index=False)
    rrp.to_csv(f"{root}rrp.csv", index=False)
    nfi.to_csv(f"{root}nfi.csv", index=False)
    operational_achievements.to_csv(f"{root}operational_achievements.csv", index=False)

    print("EA master data organized")


def organize_dref(sheet):
    root = "../organized_dref/"
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "DREF" + sheet[col_name[3]] + sheet[col_name[5]] 

    disasters = sheet.loc[:, col_name[:10]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[10:18] + col_name[31:33] + col_name[50:52]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[18:31]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[33:40]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[40:50]]

    disasters.to_csv(f"{root}disasters.csv", index=False)
    operational_progresses.to_csv(f"{root}operational_progresses.csv", index=False)
    financial_progress.to_csv(f"{root}financial_progress.csv", index=False)
    rrp.to_csv(f"{root}rrp.csv", index=False)
    operational_achievements.to_csv(f"{root}operational_achievements.csv", index=False)

    print("DREF master data organized")

def organize_mcmr(sheet):
    root = "../organized_mcmr/"
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "MCMR" + sheet[col_name[3]] + sheet[col_name[5]] 

    disasters = sheet.loc[:, col_name[:10]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[10:13] + col_name[19:21] + col_name[34:41]]
    total_coverage = sheet.loc[:, [col_name[0]] + col_name[13:19]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[21:34]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[41:43]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[43:]]

    total_coverage = total_coverage.replace('\n', ' ', regex=True)
    total_coverage = total_coverage.replace(' ', ' ', regex=True)

    disasters.to_csv(f"{root}disasters.csv", index=False)
    operational_progresses.to_csv(f"{root}operational_progresses.csv", index=False)
    total_coverage.to_csv(f"{root}total_coverage.csv", index=False)
    rrp.to_csv(f"{root}rrp.csv", index=False)
    financial_progress.to_csv(f"{root}financial_progress.csv", index=False)
    operational_achievements.to_csv(f"{root}operational_achievements.csv", index=False)
    
    print("MCMR master data organized")


def organize_protracted(sheet):
    root = "../organized_pcce/"
    col_name = sheet.columns.tolist()
    sheet["Ref"] = "PCCE" + sheet[col_name[3]] + sheet[col_name[5]]

    disasters = sheet.loc[:, col_name[:10]]
    operational_progresses = sheet.loc[:, [col_name[0]] + col_name[10:14] + col_name[20:24] + col_name[38:46] + col_name[56:57] + col_name[59:61]]
    coverage = sheet.loc[:, [col_name[0]] + col_name[14:20]]
    rrp = sheet.loc[:, [col_name[0]] + col_name[24:38]]
    dashboard = sheet.loc[:, [col_name[0]] + col_name[46:51]]
    financial_progress = sheet.loc[:, [col_name[0]] + col_name[51:56]]
    operational_achievements = sheet.loc[:, [col_name[0]] + col_name[57:59]]

    coverage = coverage.replace('\n', ' ', regex=True)
    coverage = coverage.replace(' ', ' ', regex=True)

    disasters.to_csv(f"{root}disasters.csv", index=False)
    operational_progresses.to_csv(f"{root}operational_progresses.csv", index=False)
    coverage.to_csv(f"{root}coverage.csv", index=False)
    rrp.to_csv(f"{root}rrp.csv", index=False)
    dashboard.to_csv(f"{root}dashboard.csv", index=False)
    financial_progress.to_csv(f"{root}financial_progress.csv", index=False)
    operational_achievements.to_csv(f"{root}operational_achievements.csv", index=False)

    print("PCCE master data organized")

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