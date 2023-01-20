
import json

import pendulum
from kubernetes.client import models as k8s
from airflow.decorators import dag, task
from datetime import datetime, timedelta

# context is a dictionary containing the following items:
# https://github.com/apache/airflow/blob/main/airflow/utils/context.pyi#L55

from functools import partial

# context is passed by Airflow itself
# email is user provided parameter
def send_email(email, context):
    print(f"Sending success email to {email} {context['run_id']}")

def send_email_callback(email): # users pass parameters to this function, above function is the one that actually does the handling 
    return partial(send_email, email) # returns a Callable with the user-given parameters fixed.

default_args = {
    'retries': 0,
    'on_success_callback': send_email_callback('bqnt@bloomberg.net'),
    'queue': 'kubernetes'
}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['success'],
    default_args=default_args
)
def success_callback_test():
    @task()
    def start():
        return "start task"
    @task()
    def success(_):
        return "such workflow. much orchestration."
    success(start())
success_callback_test()
