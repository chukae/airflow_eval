from time import time


try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import requests
    #from time import sleep
    #import pandas as pd
    print("All the Dag modules are fine..")

except Exception as e:
    print("Error {} ".format(e))


def first_fcn_execute(**ctx):
    ctx['ti'].xcom_push(key='mykey', value="~~Hello from ==> first_fcn_execute ~~")
    val = requests.get("https://yahoo.com")
    page = val.text
    print("First Function Execute..")
    print("@"*70)
    print(page[:100])
    #return "Hello Airflow world "+ variable

def second_fcn_execute(**ctx):
    #variable = kwargs.get("name", "Could not find key")
    #print("hello Airlow people {}".format(variable))
    instance = ctx.get("ti").xcom_pull(key="mykey")
    print("I am in second_fcn_execute I hold value {} from function 1 ".format(instance))
    #return "Hello Airflow world "

# */2 **** Execute every 2 minutes
with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args = {
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021,11,1),      
    },
    catchup=False) as f:
    

    first_fcn_execute = PythonOperator(
        task_id="first_fcn_execute",
        python_callable=first_fcn_execute,
        provide_context=True,
        op_kwargs={"name":"Chuka E"}
    )

    second_fcn_execute = PythonOperator(
        task_id="second_fcn_execute",
        python_callable=second_fcn_execute,
        provide_context=True,
        op_kwargs={"name":"Chuka E"}
    )

    first_fcn_execute >> second_fcn_execute