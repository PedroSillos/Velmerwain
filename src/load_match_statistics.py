import os
from datetime import datetime
import pandas as pd

def get_project_path(project_name:str):
    file_path = os.path.abspath(__file__)
    dir_path = os.path.dirname(file_path)
    project_path = dir_path[:dir_path.index(project_name)+len(project_name)]
    return project_path

def loadMatchStatsTable(stage_file_name,stats_file_name):
    df_match_stage = pd.read_csv(stage_file_name)

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

    df_calc_stats.to_csv(stats_file_name, index=False)

if __name__ == "__main__":
    project_name = "riot_games_analytics"
    project_path = get_project_path(project_name)
    stage_file_name = f"{project_path}/data/stage_match.csv"
    stats_file_name = f"{project_path}/data/match_stats.csv"

    loadMatchStatsTable(stage_file_name,stats_file_name)