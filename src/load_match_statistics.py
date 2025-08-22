import argparse
import os
from datetime import datetime
import pandas as pd

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_name", type=str)
    parser.add_argument("--stage_file_name", type=str)
    parser.add_argument("--stats_file_name", type=str)
    args = parser.parse_args()
    return args.project_name,args.stage_file_name,args.stats_file_name

def get_project_path(project_name:str):
    file_path = os.path.abspath(__file__)
    dir_path = os.path.dirname(file_path)
    project_path = dir_path[:dir_path.index(project_name)+len(project_name)]
    return project_path

def loadMatchStatsTable(stage_file_path,stats_file_path):
    df_match_stage = pd.read_csv(stage_file_path)

    df_match_stage['gameVersion'] = df_match_stage['gameVersion'].str.split('.').str[:2].str.join('.')

    key_columns = ["puuid", "gameVersion", "individualPosition", "championName"]
    
    df_calc_stats = df_match_stage.groupby(key_columns).agg(
        matchs=("championName", lambda x: x.count()),
        winRate=("win", lambda x: f"{x.mean() * 100:.2f}%"),
        KDA=(
              "kills",
              lambda x: (
                (df_match_stage.loc[x.index, "kills"] + df_match_stage.loc[x.index, "assists"])
                / df_match_stage.loc[x.index, "deaths"]).mean()
        ),
        averageKills=("kills", "mean"),
        averageDeaths=("deaths", "mean"),
        averageAssists=("assists", "mean"),
        averageGoldEarned=("goldEarned", "mean"),
        averageDamageDealt=("totalDamageDealt", "mean"),
        averageChampLevel=("champLevel", "mean"),
        averageVisionScore=("visionScore", "mean"),
        averageGameDuration=("gameDuration", lambda x: x.sum() / 60.00)
    ).reset_index()

    df_calc_stats["modifiedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_calc_stats.to_csv(stats_file_path, index=False)

if __name__ == "__main__":
    # How to run:
    # python .\src\load_match_statistics.py --project_name riot_games_analytics --stage_file_name stage_match.csv --stats_file_name match_stats.csv
    
    project_name, stage_file_name, stats_file_name = get_args()
    project_path = get_project_path(project_name)
    stage_file_path = f"{project_path}/data/{stage_file_name}"
    stats_file_path = f"{project_path}/data/{stats_file_name}"

    loadMatchStatsTable(stage_file_path,stats_file_path)