import argparse
import os
from datetime import datetime
import pandas as pd

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage_match_file_name", type=str)
    parser.add_argument("--stats_match_file_name", type=str)
    
    args = parser.parse_args()
    return args.stage_match_file_name, args.stats_match_file_name

def get_file_path(stage_file_name: str):
    src_path = os.path.abspath(__file__)
    src_dir_path = os.path.dirname(src_path)
    project_path = src_dir_path.replace("/src", "")
    stage_file_path = f"{project_path}/data/{stage_file_name}"

    return stage_file_path

def load_match_stats_table(stage_match_file_path, stats_match_file_path):
    df_match_stage = pd.read_csv(stage_match_file_path)

    df_match_stage['game_version'] = df_match_stage['game_version'].str.split('.').str[:2].str.join('.')

    key_columns = ["puuid", "game_version", "individual_position", "champion_name"]
    
    df_calc_stats = df_match_stage.groupby(key_columns).agg(
        matchs=("champion_name", lambda x: x.count()),
        win_rate=("win", lambda x: f"{x.mean() * 100:.2f}%"),
        kda=(
              "kills",
              lambda x: (
                (df_match_stage.loc[x.index, "kills"] + df_match_stage.loc[x.index, "assists"])
                / df_match_stage.loc[x.index, "deaths"]).mean()
        ),
        average_kills=("kills", "mean"),
        average_deaths=("deaths", "mean"),
        average_assists=("assists", "mean"),
        average_gold_earned=("gold_earned", "mean"),
        average_damage_dealt=("total_damage_dealt", "mean"),
        average_champ_level=("champ_level", "mean"),
        average_vision_score=("vision_score", "mean"),
        average_game_duration=("game_duration", lambda x: x.mean() / 60.00)
    ).reset_index()

    df_calc_stats["modified_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_calc_stats.to_csv(stats_match_file_path, index=False)

if __name__ == "__main__":
    # How to run:
    # python [...]/load_match_statistics.py --stage_match_file_name stage_match.csv --stats_match_file_name match_stats.csv
    
    stage_match_file_name, stats_match_file_name = get_args()

    stage_match_file_path = get_file_path(stage_match_file_name)
    stats_match_file_path = get_file_path(stats_match_file_name)

    load_match_stats_table(stage_match_file_path, stats_match_file_path)