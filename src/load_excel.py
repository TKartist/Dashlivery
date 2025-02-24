import pandas as pd


def load_excel(path):
    
    try:    
        xls = pd.ExcelFile(path)
        
        sheets = {sheet_name : xls.parse(sheet_name) for sheet_name in xls.sheet_names}
        
        for sheet_name in sheets.keys():
            print(sheet_name)
        
        return sheets
    except Exception as e:
        print(f"error while loading {path} for excel data: {e}")



def organize_dref(sheet):
    return "hi"


def organize_ea(sheet):
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
    filename = "dummy_data/ewts_master_dummy_data.xlsx"
    sheet_dict = load_excel(filename)
    organize_sheets(sheet_dict)
    print("run the files lads")

if __name__ == "__main__":
    main()