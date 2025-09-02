from airflow.sdk import dag, task
import os

def get_script_path(script_name: str):
    current_path = os.path.abspath(__file__)
    current_dir_path = os.path.dirname(current_path)
    project_path = current_dir_path.replace("/dags","")
    script_path = f"{project_path}/src/{script_name}"
    return script_path

@dag
def load_match_dag(api_key: str):
    
    @task.bash
    def load_match(api_key: str):
        script_path = get_script_path("load_match.py")
        stage_player_file_name = "stage_player.csv"
        stage_match_file_name = "stage_match.csv"
        
        command = f""" \
            python {script_path} \
                --stage_player_file_name {stage_player_file_name} \
                --stage_match_file_name {stage_match_file_name} \
                --api_key {api_key} \
        """

        if os.path.exists(script_path):
            return command
        return

    load_match(api_key)

load_match_dag(
    api_key="{{ var.value.api_key }}"
)