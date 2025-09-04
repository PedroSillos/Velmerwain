from airflow.sdk import dag, task
import os

def get_script_path(script_name: str):
    current_path = os.path.abspath(__file__)
    current_dir_path = os.path.dirname(current_path)
    project_path = current_dir_path.replace("/dags","")
    script_path = f"{project_path}/src/{script_name}"
    return script_path

@dag(schedule="*/10 * * * *")
def load_match_stats_dag():
    
    @task.bash
    def load_match_stats():
        script_path = get_script_path("load_match_statistics.py")
        stage_match_file_name = "stage_match.csv"
        stats_match_file_name = "match_stats.csv"
        
        command = f""" \
            python {script_path} \
                --stage_match_file_name {stage_match_file_name} \
                --stats_match_file_name {stats_match_file_name} \
        """

        if os.path.exists(script_path):
            return command
        return

    load_match_stats()

load_match_stats_dag()