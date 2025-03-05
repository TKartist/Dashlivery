import requests
import pandas as pd
from dotenv import load_dotenv
import os
import json

load_dotenv()
headers = {
    "Content-Type": "application/json"
}

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


def read_area_info_folder(folder):
    files = os.listdir(folder)
    df_list = []
    for file in files:
        if file == "general_info.csv":
            continue
        df = pd.read_csv(f"{folder}/{file}")
        df = df[["Ref", "Achieved", "Not Achieved", "Missing", "Achieved Early", "Achieved Late", "TBD", "DNU", "Data Completeness", "General Performance"]]
        df["Area"] = file.split(".")[0]
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


def read_task_info(root, file):
    df = pd.read_csv(root+file, index_col="Ref")
    print(df)
    cols = df.columns[9:].copy()
    print(cols)
    # for index, row in df.iterrows():



def task_info():
    read_task_info("../organized_ea/", "assessment.csv")

task_info()