from airflow.sdk import dag, task
import os

def get_file_path(file_name: str, file_dir: str):
    current_path = os.path.abspath(__file__)
    project_path = "/".join(current_path.split("/")[:-2])
    file_path = f"{project_path}/{file_dir}/{file_name}"
    return file_path

@dag
def load_player_dag(game_name: str, tag_line: str, api_key: str):
    
    @task.bash
    def load_raw_account(game_name: str, tag_line: str, api_key: str):
        script_path = get_file_path(file_name="load_raw_account.py", file_dir="src")
        
        command = f"python {script_path} --game_name {game_name} --tag_line {tag_line} --api_key {api_key}"

        if os.path.exists(script_path):
            return command
        else:
            return f"File '{script_path}' does not exist."
    
    @task.bash
    def load_raw_account_region(api_key: str):
        script_path = get_file_path(file_name="load_raw_account_region.py", file_dir="src")
        
        command = f"python {script_path} --api_key {api_key}"

        if os.path.exists(script_path):
            return command
        else:
            return f"File '{script_path}' does not exist."

    @task.bash
    def load_raw_summoner(api_key: str):
        script_path = get_file_path(file_name="load_raw_summoner.py", file_dir="src")
        
        command = f"python {script_path} --api_key {api_key}"

        if os.path.exists(script_path):
            return command
        else:
            return f"File '{script_path}' does not exist."
    
    load_raw_account(game_name, tag_line, api_key) >> load_raw_account_region(api_key) >> load_raw_summoner(api_key)

load_player_dag(
    game_name="{{ var.value.game_name }}",
    tag_line="{{ var.value.tag_line }}",
    api_key="{{ var.value.api_key }}"
)