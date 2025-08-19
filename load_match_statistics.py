import os
from datetime import datetime
import pandas as pd

def loadMatchStatsTable(stage_file_name,stats_file_name):
    df_match_stage = pd.read_csv(stage_file_name)

    key_columns = ["puuid", "gameVersion", "individualPosition", "championName"]
    
    df_calc_stats = df_match_stage.groupby(key_columns).agg(
        winRate=("win", "mean"),
        KDA=(
              "kills",
              lambda x: (
                (df_match_stage.loc[x.index, "kills"] + df_match_stage.loc[x.index, "assists"])
                / df_match_stage.loc[x.index, "deaths"]).sum()
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

    if not os.path.exists(stats_file_name):
        df_calc_stats.to_csv(stats_file_name, index=False)
    else:
        df_file = pd.read_csv(stats_file_name)

if __name__ == "__main__":
    stage_file_name = "stage_match.csv"
    stats_file_name = "match_stats.csv"

    loadMatchStatsTable(stage_file_name,stats_file_name)