import requests
import pandas as pd
from dotenv import load_dotenv
import os
import json

load_dotenv()
headers = {
    "Content-Type": "application/json"
}

def push_general_info():
    url = os.getenv("POWER_BI_GENERAL_URL")
    df = pd.read_csv(f"../organized_ea/general_info.csv")
    df1 = pd.read_csv(f"../organized_dref/general_info.csv")
    df2 = pd.read_csv(f"../organized_mcmr/general_info.csv")
    df3 = pd.read_csv(f"../organized_pcce/general_info.csv")

    df.rename(columns=lambda x: x.strip(), inplace=True)
    df1.rename(columns=lambda x: x.strip(), inplace=True)
    df2.rename(columns=lambda x: x.strip(), inplace=True)
    df3.rename(columns=lambda x: x.strip(), inplace=True)
    
    df2.rename(columns={df2.columns[2]: "Trigger Date"}, inplace=True)
    df3.rename(columns={df3.columns[2]: "Trigger Date"}, inplace=True)
    df_combined = pd.concat([df, df1, df2, df3])
    df["tracker Status"] = df["tracker Status"].str.upper()
    
    data = df_combined.to_dict(orient="records")
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        print("✅ Data successfully pushed to Power BI!")
    else:
        print(f"❌ Error: {response.status_code}, {response.text}")


