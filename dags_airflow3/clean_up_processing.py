# k get pods -n stackable-products | grep Completed

from airflow.sdk import dag, task
import subprocess
import importlib
import sys

print(sys.path)

# invalidate cache due to race condition when using dag-processor
importlib.reload(site)
importlib.invalidate_caches()

@dag(dag_id="clean-up-jobs", schedule="@daily", tags="clean_up")
def clean_up_completed_jobs():
    def get_completed_jobs() -> [str]:
        cmd = "/stackable/kubectl get pods -n stackable-products | grep Completed"
        output = subprocess.check_output(cmd, shell=True)
        print(output)
        return str(output).strip('b\'').split('\\n')

    def filter_spark_job_names(pod_list: [str]) -> [str]:
        pods = []
        for pod_details in pod_list:
            pod_name = pod_details.split(' ')[0]
            if pod_name != '':
                print(f"Found Pod: {pod_name}")
                pods.append(pod_name)
        return pods

    def delete_jobs(pod_names: [str]):
        for name in pod_names:
            print(f"ABOUT TO DELTE {name}")
            cmd = f"/stackable/kubectl delete pod {name} -n stackable-products"
            output = subprocess.check_output(cmd, shell=True)

    @task
    def delete_completed_spark_jobs():
        pod_names = filter_spark_job_names(get_completed_jobs())
        delete_jobs(pod_names)

    delete_completed_spark_jobs()

clean_up_completed_jobs()
