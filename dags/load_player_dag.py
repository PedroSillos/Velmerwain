from airflow.sdk import dag, task
import os

@dag
def load_player_dag(gameName: str, tagLine: str, region:str, apiKey: str):
    
    @task.bash
    def load_player(gameName: str, tagLine: str, region:str, apiKey: str):
        current_path = os.path.abspath(__file__)
        current_dir_path = os.path.dirname(current_path)
        project_path = current_dir_path.replace("/dags","")
        script_name = "load_player.py"
        script_path = f"{project_path}/src/{script_name}"
        
        command = f""" \
            python {script_path} \
                --stageFileName stage_player.csv \
                --gameName {gameName} \
                --tagLine {tagLine} \
                --region {region} \
                --apiKey {apiKey} \
        """

        if os.path.exists(script_path):
            return command
        return

    load_player(gameName, tagLine, region, apiKey)

load_player_dag(
    gameName="{{ var.value.game_name }}",
    tagLine="{{ var.value.tag_line }}",
    region="{{ var.value.region }}",
    apiKey="{{ var.value.api_key }}"
)