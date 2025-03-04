import requests
import pandas as pd
from dotenv import load_dotenv
import os
import json

ea_folder = "../organized_ea/"
dref_folder = "../organized_dref/"
mcmr_folder = "../organized_mcmr/"
pcce_folder = "../organized_pcce/"

def general_info():
    filename = "general_info.csv"
    df = pd.read_csv(f"{ea_folder}{filename}")
    df1 = pd.read_csv(f"{dref_folder}{filename}")
    df2 = pd.read_csv(f"{mcmr_folder}{filename}")
    df3 = pd.read_csv(f"{pcce_folder}{filename}")

    df.rename(columns=lambda x: x.strip(), inplace=True)
    df1.rename(columns=lambda x: x.strip(), inplace=True)
    df2.rename(columns=lambda x: x.strip(), inplace=True)
    df3.rename(columns=lambda x: x.strip(), inplace=True)
    df["tracker Status"] = df["tracker Status"].str.upper()
    
    df2.rename(columns={df2.columns[2]: "Trigger Date"}, inplace=True)
    df3.rename(columns={df3.columns[2]: "Trigger Date"}, inplace=True)
    df_combined = pd.concat([df, df1, df2, df3])
    
    df_combined.to_csv("../power_bi_input/operation_summaries.csv", index=False)

    return df_combined


def read_area_info_folder(folder):
    files = os.listdir(folder)
    df_list = []
    for file in files:
        if file == "general_info.csv":
            continue
        df = pd.read_csv(f"{folder}/{file}")
        df["Area"] = file.split(".")[0]
        df = df[["Ref", "Area", "Achieved", "Not Achieved", "Missing", "Achieved Early", "Achieved Late", "TBD", "DNU", "Data Completeness", "General Performance"]]
        df_list.append(df)
    
    df_combined = pd.concat(df_list)
    return df_combined


def area_info():
    df = read_area_info_folder(ea_folder)
    df1 = read_area_info_folder(dref_folder)
    df2 = read_area_info_folder(mcmr_folder)
    df3 = read_area_info_folder(pcce_folder)
    df_combined = pd.concat([df, df1, df2, df3])
    df_combined.to_csv("../power_bi_input/area_summaries.csv", index=False)
    
    return df_combined

def read_task_info(root, file):
    df = pd.read_csv(root+file, index_col="Ref")
    cols = df.columns[9:].copy()
    area = file.split(".")[0]
    task_infos = []
    for index, row in df.iterrows():
        for a, b in zip(cols[::2], cols[1::2]):
            task_infos.append({
                "Ref" : index,
                "Area" : area,
                "Task" : a,
                "Status" : row[a],
                "Delta" : row[b]
            })
    return task_infos


def areas_in_op(folder):
    files = os.listdir(folder)
    task_infos = []
    for file in files:
        if file == "general_info.csv" or file == "information_management.csv":
            continue
        task_infos += read_task_info(folder, file)
    return task_infos


def task_info():
    task_infos = areas_in_op(ea_folder) + areas_in_op(dref_folder) + areas_in_op(mcmr_folder) + areas_in_op(pcce_folder)
    df = pd.DataFrame(task_infos)
    df.to_csv("../power_bi_input/task_summaries.csv", index=False)

    return df


def generate_powerbi_input():
    general = general_info()
    area = area_info()
    task = task_info()

    with pd.ExcelWriter("../power_bi_input/input.xlsx", engine="xlsxwriter") as writer:
        general.to_excel(writer, sheet_name="general_information", index=False)
        area.to_excel(writer, sheet_name="area_info", index=False)
        task.to_excel(writer, sheet_name="task_info", index=False)


generate_powerbi_input()