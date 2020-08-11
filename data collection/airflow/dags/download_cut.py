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
from datetime import timedelta
import datetime
import logging
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import time

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
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
    'download_cut',
    default_args=default_args,
    description='Download audios from file',
    #schedule_interval=timedelta(days=1),
    schedule_interval=None,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
def sleep(seconds,**kwargs):
    time.sleep(seconds)


rootFolder=Variable.get("rootFolder")
downloadFileName =Variable.get("downloadFileName")
templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env input_file={{var.value.downloadFileName }} --env output_folder=./data/download_output download_youtube
"""
t1 = BashOperator(
    task_id='Download_Audios',
    bash_command=templated_command,
    dag=dag,
)
t3 =PythonOperator(
    task_id="sleep",
    provide_context=True,
    python_callable=sleep,
    op_kwargs={'seconds': 3600},
    dag=dag,
)  
templated_command = """
airflow unpause downloadMonitor


"""
t4=BashOperator(
    task_id="unpause_downloadMonitor",
    bash_command=templated_command,
    dag=dag

)

templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=clear_space.py --env input_folder=data/download_output/webm --env obs_folder=data/cut_input post_download_process


"""
t5 =BashOperator(
    task_id="clean_file_name",
    bash_command=templated_command,
    dag=dag
)
templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=upload_to_obs.py --env input_folder=data/cut_input --env obs_folder=download-raw post_download_process
"""
t6 =BashOperator(
    task_id="upload_to_obs",
    bash_command=templated_command,
    dag=dag
)

templated_command = """
docker run -v {{var.value.rootFolder}}:/app --env function_file=folderbased_cut_speech.py --env output_folder=./data/cut_output --env input_folder=./data/cut_input cut_speech
"""
t8 =BashOperator(
    task_id="cut_speech",
    bash_command=templated_command,
    dag=dag
)
templated_command = """
airflow unpause APIcall


"""
t9=BashOperator(
    task_id="unpause_API_call",
    bash_command=templated_command,
    dag=dag

)
dag >> t1 >> t5 >> t6 >> t8 >> t9
dag >> t3 >> t4
#dag >> t8 >> t9
