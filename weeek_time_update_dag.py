import requests as rq
import pandas as pd
from functools import reduce
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import os
from os.path import join, dirname
from dotenv import load_dotenv
from datetime import datetime

# get env variables
# dotenv_path = join(dirname(__file__), '.env')
# load_dotenv(dotenv_path)
# BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
# BASE_URL = os.environ.get("BASE_URL")
# DB_USER = os.environ.get("DB_USER")
# DB_PASSWORD = os.environ.get("DB_PASSWORD")
# DB_HOST = os.environ.get("DB_HOST")
# DB_PORT = os.environ.get("DB_PORT")
# DB_NAME = os.environ.get("DB_NAME")
# DB_SCHEMA = os.environ.get("DB_SCHEMA")



# function to get all items from weeek over groups
def get_all_items(url_part, item_type): 
    token = Variable.get('BEARER_TOKEN_WEEEK_UPD')
    base_url = Variable.get('BASE_URL_WEEEK_UPD')
    headers={'Authorization': f'Bearer {token}', 'Content-Type':	'application/json'}
    response = rq.get(base_url + url_part, headers=headers)
    if 'hasMore' in response.json():
        all_items = [] 
        [all_items.append(x) for x in response.json()[item_type]]
        offset = 0
        while response.json()['hasMore']:
            offset += 1
            response = rq.get(BASE_URL + url_part + f'?offset={20 * offset}', headers=headers)
            [all_items.append(x) for x in response.json()[item_type]]
        return all_items
    else:
        return response.json()[item_type]
    
# Get all items of tags, tasks, project, members
def pull_data_from_weeek(task_instance):
    tags = get_all_items('ws/tags', 'tags')
    tasks = get_all_items('tm/tasks', 'tasks')
    projects = get_all_items('tm/projects', 'projects')
    members = get_all_items('ws/members', 'members')
    task_instance.xcom_push(key='tasks', value=tasks)
    task_instance.xcom_push(key='tags', value=tags)
    task_instance.xcom_push(key='members', value=members)
    task_instance.xcom_push(key='projects', value=projects)


def push_data_to_db(task_instance):
    user=Variable.get('DB_USER_WEEEK_UPD')
    passwd=Variable.get('DB_PASSWORD_WEEEK_UPD')
    host=Variable.get('DB_HOST_WEEEK_UPD')
    port=Variable.get('DB_PORT_WEEEK_UPD')
    db_name=Variable.get('DB_NAME_WEEEK_UPD')
    db_schema=Variable.get('DB_SCHEMA_WEEEK_UPD')

    # create connection
    engine = create_engine(f"postgresql://{user}:{passwd}@{host}:{port}/{db_name}")

    tags = pd.DataFrame(task_instance.xcom_pull(key = 'tags'))
    tasks = pd.DataFrame(task_instance.xcom_pull(key = 'tasks'))
    members = pd.DataFrame(task_instance.xcom_pull(key = 'members'))
    projects = pd.DataFrame(task_instance.xcom_pull(key = 'projects'))

    # get last data from weeek_mark

    with engine.connect() as conn:
        last_date = conn.execute(text(r'''select max(wm."writeOffDate") as max_date from dds.weeek_mart wm''')).fetchone()[0]

    # ... and delete rows from table over last_date
    with engine.connect() as conn:
        conn.execute(text(f'''delete from dds.weeek_mart wm where wm."writeOffDate" = '{last_date}' '''))

    with engine.connect() as conn:
        employees = pd.DataFrame(conn.execute(text("select e.name, concat(split_part(e.name, ' ', 1), ' ', split_part(e.name, ' ', 2)) as employee from dds.employees e")).fetchall())

    # append new data
    with engine.connect() as conn:
        (
            tasks
            .assign(tags = lambda x: x['tags'].apply(lambda item: reduce(lambda a, b: a + ', ' + b, [tags.query(f'id == {x}')['title'].to_list()[0] for x in item]) if len(item) > 0 else ''))
            .assign(head = lambda x: 'Леонов Александр Владимирович')
            .assign(dept = lambda x: 'Фабрика данных')
            .assign(project = lambda x: x['projectId'].astype('Int64').fillna(0).apply(lambda x: projects.query(f'id == {x}').values[0][2] if x != 0 else 'Прочее'))
            .assign(parentTask = lambda x: x['parentId'].fillna(0).apply(lambda x: tasks.query(f"id == {x}").values[0][2] if x != 0 and tasks.query(f'id == {x}').shape[0] > 0 else None))
            .assign(subTask = lambda x: x['subTasks'].apply(lambda x: tasks.query(f"id == {x[0]}").values[0][2] if len(x) > 0 and tasks.query(f'id == {x[0]}').shape[0] > 0 else None))
            .assign(subTask2 = lambda x: x['subTasks'].apply(lambda x: tasks.query(f"id == {x[1]}").values[0][2] if len(x) > 1 and tasks.query(f'id == {x[1]}').shape[0] > 0 else None))
            .assign(timeId = lambda x: x['timeEntries'].apply(lambda x: [x['id'] for x in x] if len(x) > 0 else []))
            .assign(employee = lambda x: x['timeEntries'].apply(lambda x: [' '.join(list(members.query(f"id == '{x['userId']}'").values[0])[3:5]) for x in x] if len(x) > 0 else []))
            .assign(writeOffDate = lambda x: x['timeEntries'].apply(lambda x: [x['date'] for x in x] if len(x) > 0 else []))
            .assign(workTime = lambda x: x['timeEntries'].apply(lambda x: [round(x['duration'] / 60, 4) for x in x] if len(x) > 0 else []))
            .assign(commentText = lambda x: x['timeEntries'].apply(lambda x: [x['comment'] for x in x] if len(x) > 0 else []))
            [['timeId', 'id', 'head', 'dept', 'project', 'parentTask', 'title', 'subTask', 'subTask2', 'tags', 'commentText', 'employee', 'writeOffDate', 'workTime']]
            .explode(column = ['timeId', 'commentText', 'employee', 'writeOffDate', 'workTime'])
            .query(f"(~timeId.isna()) and (writeOffDate > '{last_date}')")
            .sort_values(['writeOffDate'], ascending=[False])
            .reset_index(drop=True)
            .assign(writeOffDate = lambda x: x['writeOffDate'].astype('datetime64[ns]').date())
            .merge(employees, how='left', on = 'employee')
            .assign(employee = lambda x: x['name'])
            .drop(columns = ['name'])
            .to_sql('weeek_mart', conn, schema='dds', if_exists = 'append', index=False)
        )

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 24)    
}

with DAG(
    'weeek_mart_upd',
    default_args=default_args,
    catchup=False,
    max_active_runs =1,
    description='DAG to fetch writing off time for tasks from WEEEK and store them in skud_va',
    schedule_interval='@daily'
) as dag:

    # Определение задач
    pull_data_from_weeek = PythonOperator(
        task_id='pull_data',
        python_callable=pull_data_from_weeek,
        dag=dag,
        do_xcom_push=True
    )

    push_data_to_db = PythonOperator(
        task_id='push_data',
        python_callable=push_data_to_db,
        dag=dag,
        do_xcom_push=True
    )

    # Order of tasks
    pull_data_from_weeek >> push_data_to_db







    