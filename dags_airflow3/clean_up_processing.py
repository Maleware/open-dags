# k get pods -n stackable-products | grep Completed

from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.utils import yaml
import os
import sys
import importlib
import site
import json

print(sys.path)

# invalidate cache due to race condition when using dag-processor
importlib.reload(site)
importlib.invalidate_caches()

@dag(dag_id="clean-up-jobs", schedule="@daily", tags="clean_up")
def clean_up_completed_jobs():
    @task.bash(output_processor=lambda output: json.loads(output))
    def get_completed_jobs() -> str:
        return "/stackable/kubectl get pods -n stackable-products --output=json | jq '.items[] | select(.metadata.labels.\"app.kubernetes.io/component\" == \"spark\")'"
    @task.bash
    def delete_completed_tasks(list: str):
        return f"/stackable/kubectl delete pod -n stackable-products {list}"
        
    completed_tasks = get_completed_jobs()
    
    delete_completed_tasks(completed_tasks)

clean_up_completed_jobs()

