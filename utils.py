from sparkmeasure import StageMetrics
stagemetrics = StageMetrics(spark) 

def clean_column_names(df):
    tempList = []
    for col in df.columns:
        new_name = col.strip()
        new_name = "".join(new_name.split())
        new_name = new_name.replace('.','') 
        tempList.append(new_name) 

    return df.toDF(*tempList) 

def evaluate_metrics(task, spark_session, path):
    stagemetrics.begin()
    task(spark_session, path)
    stagemetrics.print_report()
    stagemetrics.end()
