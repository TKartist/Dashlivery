from load_excel import load_excel, organize_sheets
from overview_dataset import generate_overview
from power_bi_input import generate_powerbi_input


def main():
    filename = "../dummy_data/ewts_master_dummy_data.xlsx"
    sheets = load_excel(filename)
    buckets = organize_sheets(sheets)
    generate_overview(buckets, sheets)
    generate_powerbi_input()

if __name__ == "__main__":
    main()