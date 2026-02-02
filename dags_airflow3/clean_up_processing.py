# k get pods -n stackable-products | grep Completed

from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.utils import yaml
import os
import sys
import importlib
import site

print(sys.path)

# invalidate cache due to race condition when using dag-processor
importlib.reload(site)
importlib.invalidate_caches()

@dag(dag_id="clean-up-jobs", schedule="@daily", tags="clean_up"):
def clean_up_completed_jobs():
    @task.bash
    get_completed_jobs() -> str:
        return "/stackable/kubectl get pods -n stackable-products | grep Completed"

    @task
    delete_completed_tasks(list: str):
        print(f'{list}')
        
    completed_tasks = get_completed_jobs()
    
    delete_completed_tasks(completed_tasks)

clean_up_completed_jobs()
