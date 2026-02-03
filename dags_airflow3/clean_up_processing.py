from airflow.sdk import dag, task
import subprocess
import importlib
import logging
import sys
import site

import json

print(sys.path)

# invalidate cache due to race condition when using dag-processor
importlib.reload(site)
importlib.invalidate_caches()

TARGET_NAMESPACE="stackable-products"

@dag(dag_id="clean-up-jobs", schedule="@daily", tags="cleanUp")
def clean_up_completed_jobs():
    def get_completed_jobs() -> [str]:
        """
        Output of kubectl get pods looks like
        NAME                                                READY   STATUS      RESTARTS        AGE
        airflow-postgresql-0                                1/1     Running     0               3h57m
        airflow-scheduler-default-0                         3/3     Running     0               3h51m
        airflow-triggerer-default-0                         3/3     Running     1 (3h51m ago)   3h51m
        airflow-webserver-default-0                         3/3     Running     2 (3h50m ago)   3h51m
        pyspark-pi-20260203125544-e0a3689c2392e824-driver   0/1     Completed   0               76s

        grep for "Completed" only leaves lines with the podnames sperated by \\n"
        """
        cmd = f"/stackable/kubectl get pods -n {TARGET_NAMESPACE} | grep Completed"
        output = subprocess.check_output(cmd, shell=True)
        json_string = json.loads(output)
        print(f"json: {json_string}")
        logging.debug(f"Cought pods from {TARGET_NAMESPACE}: {output}")
        # Output of subrocess contains leading b' from underlying data type.
        # Need to strip and split in lines per detected job
        return str(output).strip('b\'').split('\\n')

    def filter_spark_job_names(pod_list: [str]) -> [str]:
        pods = []
        for pod_details in pod_list:
            # Split by ' ' leaves a trailing empty entry
            pod_name = pod_details.split(' ')[0]
            if pod_name != '':
                logging.debug(f"Found Pod: {pod_name}")
                pods.append(pod_name)
        return pods

    def delete_jobs(pod_names: [str]):
        for name in pod_names:
            logging.info(f"Deleting {name}")
            cmd = f"/stackable/kubectl delete pod {name} -n {TARGET_NAMESPACE}"
            output = subprocess.check_output(cmd, shell=True)

    @task
    def delete_completed_spark_jobs():
        pod_names = filter_spark_job_names(get_completed_jobs())
        delete_jobs(pod_names)

    delete_completed_spark_jobs()

clean_up_completed_jobs()
