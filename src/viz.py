df = spark.sql("SELECT * FROM master_data_processing.task_info LIMIT 1000")

viz = df.groupby('Task').mean().toPandas()

viz = viz.dropna(subset=['avg(Score)'])
viz = viz[viz["avg(Score)"] != 0]
viz = viz.sort_values(by='avg(Score)')

scores = viz['avg(Score)']

colors = [
    'red' if score < 3 else 
    'orange' if score < 4 else 
    '#37FD12'
    for score in scores
]
plt.figure(figsize=(15, 16), dpi=700)
plt.barh(viz["Task"], viz['avg(Score)'], color=colors)
plt.show()
