from airflow.sdk import dag, task
import os

@dag
def load_player_dag(apiKey: str):
    
    @task.bash
    def load_player(apiKey: str):
        current_path = os.path.abspath(__file__)
        current_dir_path = os.path.dirname(current_path)
        project_path = current_dir_path.replace("/dags","")
        script_name = "load_player.py"
        script_path = f"{project_path}/src/{script_name}"
        
        command = f"""
            python {script_path} \
                --stageFileName stage_player.csv \
                --gameName OTalDoPedrinho \
                --tagLine BR1 \
                --region americas \
                --apiKey {apiKey}
        """

        if os.path.exists(script_path):
            return command
        return

    load_player(apiKey)

load_player_dag(<apiKey>)