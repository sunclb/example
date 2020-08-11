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
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import time

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 1),
    'retries': 0,

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
    'monitor',
    default_args=default_args,
    description='Monitor datapipline',
    schedule_interval='*/30 * * * *',
    #schedule_interval=timedelta(minutes=30),
    catchup=False,
    max_active_runs=1
    #schedule_interval=@hourly,
    #schedule_interval='@once'
    #'max_active_runs':1,
    #schedule_interval=None
)
def mp3gen(folder):
    file_num=0
    for root, dirs, files in os.walk(folder):
        for filename in files:
	        file_num+=1
    return file_num

def check_complete(**kwargs):
    download_folder="data/download_output/webm"
    cut_input="data/cut_input"
    convert_wav="data/cut_input/wav"
    cut_output="data/cut_output"
    api_input="data/API_input"
    download=mp3gen(download_folder)
    cut_input=mp3gen(cut_input)
    convert_wav=mp3gen(convert_wav)
    cut_output=mp3gen(cut_output)
    api_input=mp3gen(api_input)
    if download==0 and cut_input==0 and convert_wav==0 and cut_output==0 and api_input==0:
        return "trigger_end"
    else: return "monitor"
    
rootFolder=Variable.get("rootFolder")
downloadFileName =Variable.get("downloadFileName")
branching = BranchPythonOperator(
    task_id='check_complete_signal',
    python_callable=check_complete,
    dag=dag,
)
templated_command = """
docker run -v {{var.value.rootFolder}}:/app monitor
"""
t1 = BashOperator(
    task_id='monitor',
    bash_command=templated_command,
    dag=dag,
)
templated_command = """
airflow unpause end_pipline
"""
t2 = BashOperator(
    task_id='trigger_end',
    bash_command=templated_command,
    dag=dag,
)


dag >> branching >> [t1, t2]
