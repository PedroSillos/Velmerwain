from airflow.sdk import dag, task
import os

def get_script_path(script_name: str):
    current_path = os.path.abspath(__file__)
    current_dir_path = os.path.dirname(current_path)
    project_path = current_dir_path.replace("/dags","")
    script_path = f"{project_path}/src/{script_name}"
    return script_path

@dag
def load_player_dag(game_name: str, tag_line: str, api_key: str):
    
    @task.bash
    def load_player(game_name: str, tag_line: str, api_key: str):
        script_path = get_script_path("load_player.py")
        stage_file_name = "stage_player.csv"
        
        command = f""" \
            python {script_path} \
                --stage_file_name {stage_file_name} \
                --game_name {game_name} \
                --tag_line {tag_line} \
                --api_key {api_key} \
        """

        if os.path.exists(script_path):
            return command
        return

    load_player(game_name, tag_line, api_key)

load_player_dag(
    game_name="{{ var.value.game_name }}",
    tag_line="{{ var.value.tag_line }}",
    api_key="{{ var.value.api_key }}"
)