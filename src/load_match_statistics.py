import argparse
import os
from datetime import datetime
import pandas as pd

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stageMatchFileName", type=str)
    parser.add_argument("--statsMatchFileName", type=str)
    
    args = parser.parse_args()
    return args.stageMatchFileName, args.statsMatchFileName

def get_file_path(stageFileName:str):
    srcPath = os.path.abspath(__file__)
    srcDirPath = os.path.dirname(srcPath)
    projectPath = srcDirPath.replace("\\src","")
    stageFilePath = f"{projectPath}\\data\\{stageFileName}"

    return stageFilePath

def loadMatchStatsTable(stageMatchFilePath,statsMatchFilePath):
    df_match_stage = pd.read_csv(stageMatchFilePath)

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
        averageGameDuration=("gameDuration", lambda x: x.mean() / 60.00)
    ).reset_index()

    df_calc_stats["modifiedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_calc_stats.to_csv(statsMatchFilePath, index=False)

if __name__ == "__main__":
    # How to run:
    # python src\load_match_statistics.py --stageMatchFileName stage_match.csv --statsMatchFileName match_stats.csv
    
    stageMatchFileName, statsMatchFileName = get_args()

    stageMatchFilePath = get_file_path(stageMatchFileName)
    statsMatchFilePath = get_file_path(statsMatchFileName)

    loadMatchStatsTable(stageMatchFilePath,statsMatchFilePath)