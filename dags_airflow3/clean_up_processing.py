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
        # return "/stackable/kubectl get pods -n stackable-products --output=json | jq -c '.items[] | select(.metadata.labels.\"app.kubernetes.io/component\" == \"spark\")'"
        return "/stackable/kubectl get pods -n stackable-products --output=json output.json"
    @task
    def filter_completed_spark_jobs(pods_list: list):
        for pod in pods_list:
            print(f"PODS: {pod}")
        
    completed_tasks = get_completed_jobs()
    
    filter_completed_spark_jobs(completed_tasks)

clean_up_completed_jobs()
