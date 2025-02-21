import pandas as pd


def load_excel(path):
    xls = pd.ExcelFile(path)
    
    # dict for sheets in excel file
    sheets = {sheet_name : xls.parse(sheet_name) for sheet_name in xls.sheet_names}
    
    for sheet_name, sheet in sheets.items():
        print(sheet_name)
        print(sheet.columns)

load_excel("../dummy_data/ewts_master_dummy_data.xlsx")