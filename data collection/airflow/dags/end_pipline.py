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
    'end_pipline',
    default_args=default_args,
    description='pause all dags',
    #schedule_interval=timedelta(days=1),
    schedule_interval='@once'
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
def sleep(seconds,**kwargs):
    time.sleep(seconds)
rootFolder=Variable.get("rootFolder")
downloadFileName =Variable.get("downloadFileName")
t0 =PythonOperator(
    task_id="sleep",
    provide_context=True,
    python_callable=sleep,
    op_kwargs={'seconds': 3600},
    dag=dag,
)
templated_command = """
airflow pause download_cut 


"""
t1=BashOperator(
    task_id="pause_download_call",
    bash_command=templated_command,
    dag=dag

)
templated_command = """
airflow pause APIcall  


"""
t2=BashOperator(
    task_id="pause_API_call",
    bash_command=templated_command,
    dag=dag

)

templated_command="""
docker run -v {{var.value.rootFolder}}:/app --env function_file=add_source_field.py post_download_process
"""

t4=BashOperator(
    task_id="add_source_field",
    bash_command=templated_command,
    dag=dag
)
templated_command = """
airflow pause monitor


"""
t3=BashOperator(
    task_id="pause_monitor",
    bash_command=templated_command,
    dag=dag

)
dag >> t0 >> t1 >> t2 >> t4 >> t3


