df = spark.sql("SELECT * FROM master_data_processing.task_info LIMIT 1000")
df = df[df["EWTS_Varient"] != "mcmr"]
df = df[df["Ref"] != "DREFIran, Islamic Republic ofMDRIR015"]
df = df[df["Ref"] != "DREFSyrian Arab RepublicMDRSY015"]

viz = df.groupby('Task').mean().toPandas()

viz = viz.dropna(subset=['avg(Score)'])
viz = viz[viz["avg(Score)"] != 0]
viz = viz.sort_values(by='avg(Score)')
# viz = viz[-7:]

scores = viz['avg(Score)']

colors = [
    'red' if score < 2 else 
    'orange' if score < 3 else 
    '#37FD12'
    for score in scores
]
plt.figure(figsize=(14, 23), dpi=800)
bars = plt.barh(viz["Task"], viz['avg(Score)'], color=colors)

# Add value labels to the bars
for bar in bars:
    plt.text(
        bar.get_width() + 0.03,         # x-position (a bit to the right of the bar)
        bar.get_y() + bar.get_height()/2,  # y-position (center of the bar)
        f'{bar.get_width():.2f}',       # Format the value with 2 decimal places
        va='center'
    )

plt.show()
