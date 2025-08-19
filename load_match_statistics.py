import requests
import csv
import os
from datetime import datetime
import pandas as pd

def loadMatchStatsTable(stage_file_name):
    df_match_stage = pd.read_csv(stage_file_name)
    
    header = ["puuid", "gameVersion", "individualPosition", "championName", "winRate", "KDA", "averageKills", "averageDeaths", "averageAssists", "averageGoldEarned", "averageDamageDealt", "averageChampLevel", "averageVisionScore", "averageGameDuration", "datetime"]
    
    df_match_stats = pd.DataFrame(columns=header)

    df_match_stats["puuid"] = df_match_stage["puuid"]
    df_match_stats["gameVersion"] = df_match_stage["gameVersion"]
    df_match_stats["individualPosition"] = df_match_stage["individualPosition"]
    df_match_stats["championName"] = df_match_stage["championName"]

    print(df_match_stats)

if __name__ == "__main__":
    stage_file_name = "stage_match.csv"

    loadMatchStatsTable(stage_file_name)