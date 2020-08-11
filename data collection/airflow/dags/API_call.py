# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.incubator.apache.org/tutorial.html)
"""
from datetime import datetime, timedelta
import logging
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import time
import subprocess

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 1),
    'retries': 0
    #'start_date': airflow.utils.dates.days_ago(2),
    #'email': ['sunclb.sun@gmail.com'],
    #'email_on_failure': True,
    #'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'APIcall',
    default_args=default_args,
    description='call api',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1
    #schedule_interval=None,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
def sleep(seconds,**kwargs):
    time.sleep(seconds)
def change_to_home(**kwargs):
    os.chdir(os.getenv("HOME"))
    current_path=os.getcwd()
    logging.info("now i am at directory: "+current_path)
def run_API_call(**kwargs):

    subprocess.run("./audio2text")


rootFolder=Variable.get("rootFolder")
downloadFileName =Variable.get("downloadFileName")
templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=APImove.py --env input_folder=data/cut_output --env obs_folder=data/API_input post_download_process
"""
t1 = BashOperator(
    task_id='APImove',
    bash_command=templated_command,
    dag=dag,
)


templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=callAPI.py post_download_process
"""
t3=BashOperator(
    task_id="APIcall",
    bash_command=templated_command,
    dag=dag

)

templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=post_upload_to_obs.py --env input_folder=data/API_input --env obs_folder=processed/pending-verification post_download_process


"""
t4=BashOperator(
    task_id="Post_APIcall_upload",
    bash_command=templated_command,
    dag=dag

)
templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=callAPI_rework.py post_download_process


"""
t5=BashOperator(
    task_id="call_API_rework",
    bash_command=templated_command,
    dag=dag

)

dag >> t1 >> t3>> t4 >> t5

