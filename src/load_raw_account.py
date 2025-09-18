import argparse
import os
import requests
from datetime import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--game_name", type=str)
    parser.add_argument("--tag_line", type=str)
    parser.add_argument("--api_key", type=str)

    args = parser.parse_args()
    return args.game_name, args.tag_line, args.api_key

def get_file_path(file_name: str, file_dir: str):
    current_path = os.path.abspath(__file__)
    project_path = "/".join(current_path.split("/")[:-2])
    file_path = f"{project_path}/{file_dir}/{file_name}"
    return file_path

def get_account_data(game_name: str, tag_line: str, api_key: str):
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def save_account_data(file_path: str, account_data: dict):
    spark = SparkSession.builder.appName("AccountDataUpdater").getOrCreate()

    # Add timestamp
    account_data['modifiedOn'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    # Convert to DataFrame
    new_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(account_data)]))

    # If file exists, load existing data
    if os.path.isfile(file_path):
        existing_df = spark.read.json(file_path)

        # Update rows where puuid matches
        updated_df = (
            existing_df.alias("old")
            .join(new_df.alias("new"), on="puuid", how="outer")
            .select(
                when(col("new.puuid").isNotNull(), col("new.puuid")).otherwise(col("old.puuid")).alias("puuid"),
                when(col("new.gameName").isNotNull(), col("new.gameName")).otherwise(col("old.gameName")).alias("gameName"),
                when(col("new.tagLine").isNotNull(), col("new.tagLine")).otherwise(col("old.tagLine")).alias("tagLine"),
                when(col("new.modifiedOn").isNotNull(), col("new.modifiedOn")).otherwise(col("old.modifiedOn")).alias("modifiedOn"),
            )
        )
    else:
        updated_df = new_df

    # Ensure parent dir exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Overwrite file
    updated_df.coalesce(1).write.mode("append").json(file_path)

if __name__ == "__main__":
    game_name, tag_line, api_key = get_args()
    account_data = get_account_data(game_name, tag_line, api_key)
    file_path = get_file_path(file_name="raw_account", file_dir="bronze")
    save_account_data(file_path, account_data)