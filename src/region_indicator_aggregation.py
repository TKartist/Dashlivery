df_general = spark.sql("SELECT * FROM master_data_processing.general_info LIMIT 1000").toPandas()
df_task = spark.sql("SELECT * FROM master_data_processing.task_info LIMIT 1000").toPandas()

merged_df = df_task.merge(df_general[["Ref", "Region"]], on="Ref", how='right')
merged_df = merged_df[merged_df["Status"] != "N/A"]

drefs = merged_df[merged_df["EWTS_Varient"] == "dref"]
ea = merged_df[merged_df["EWTS_Varient"] == "ea"]

y = drefs.groupby(["Task", "Region"]).agg(
    Average_Score=("Score", "mean"),
    Frequency=("Score", "count")
).reset_index().sort_values(by=["Region", "Average_Score"], ascending=False)
display(y)
z = ea.groupby(["Task", "Region"]).agg(
    Average_Score=("Score", "mean"),
    Frequency=("Score", "count")
).reset_index().sort_values(by=["Region", "Average_Score"], ascending=False)
display(z)