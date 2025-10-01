from pyspark.sql import SparkSession
import pyspark.sql.functions as SparkFunctions
from delta import configure_spark_with_delta_pip
import requests
import getpass
from datetime import datetime
import os
import time

def get_user_input():
    game_name = input("Game Name: ")
    tag_line = input("Tag Line: ")
    api_key = getpass.getpass("API Key: ")
    return game_name, tag_line, api_key

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def save_player_bronze(spark, game_name, tag_line, api_key):
    
    try:
        df = spark.read.format("delta").load("data/bronze/players")
        existing = df.filter(
            (SparkFunctions.upper(df.gameName) == game_name.upper())
            & (SparkFunctions.upper(df.tagLine) == tag_line.upper())
        )

        if existing.count() > 0:
            stored_gameName = existing.select("gameName").collect()[0]["gameName"]
            stored_tagLine = existing.select("tagLine").collect()[0]["tagLine"]
            df.createOrReplaceTempView("players")
            spark.sql(f"""
                UPDATE players 
                SET modifiedOn = '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' 
                WHERE gameName = '{game_name}' AND tagLine = '{tag_line}'
            """)
            print(f"Player {stored_gameName}#{stored_tagLine} updated")
            return
    except:
        pass
    
    player_data = {}
    
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    response = requests.get(url, headers={"X-Riot-Token": api_key})
    
    if response.status_code == 200:
        data = response.json()
        player_data = [{
            "puuid": data["puuid"],
            "gameName": data["gameName"],
            "tagLine": data["tagLine"],
            "lol-region": None,
            "modifiedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
    else:
        print(f"API Error: {response.status_code}")
        return

    url = f"https://americas.api.riotgames.com/riot/account/v1/region/by-game/lol/by-puuid/{player_data[0]["puuid"]}"
    response = requests.get(url, headers={"X-Riot-Token": api_key})
    
    if response.status_code == 200:
        data = response.json()
        player_data[0]["lol-region"] = data["region"]
    else:
        print(f"API Error: {response.status_code}")
        return
    
    df = spark.createDataFrame(player_data)
    df.write.format("delta").mode("append").save("data/bronze/players")
    print(f"Player {player_data[0]['gameName']}#{player_data[0]['tagLine']} saved")

def save_match_ids_bronze(spark, api_key):
    super_region_map = {
        "americas": ["na1", "br1", "la1", "la2"],
        "asia": ["kr", "jp1"],
        "europe": ["eun1", "euw1", "me1", "tr1", "ru"],
        "sea": ["oc1", "sg2", "tw2", "vn2"]
    }
    
    players_df = spark.read.format("delta").load("data/bronze/players")
    players = []
    
    for row in players_df.collect():
        players.append(
            {
                "puuid": row["puuid"],
                "gameName": row["gameName"],
                "tagLine": row["tagLine"],
                "lol-region": row["lol-region"]
            }
        )
    
    if not players:
        print("No players found")
        return
    
    match_id_data = []
    
    for player in players:
        start = 0
        
        player_super_region = ""
        for super_region in super_region_map:
            if player["lol-region"] in super_region_map[super_region]:
                player_super_region = super_region
        
        match_ids = ['dummy']
        while match_ids != []:
            url = f"https://{player_super_region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{player["puuid"]}/ids?queue=420&type=ranked&start={start}&count=100"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
            
            if response.status_code == 200:
                match_ids = response.json()
                for match_id in match_ids:
                    match_id_data.append({
                        "puuid": player["puuid"],
                        "matchId": match_id,
                        "modifiedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                start += 100
    
    if match_id_data:
        df_new_match_id_data = spark.createDataFrame(match_id_data)
        df_new_match_id_data.write.format("delta").mode("overwrite").save("data/bronze/match_ids")
        match_ids_by_puuid = df_new_match_id_data.groupBy("puuid").count().collect()
        for player in players:
            for row in match_ids_by_puuid:
                if row.puuid == player["puuid"]:
                    print(f"Saved {row['count']} match IDs for {player["gameName"]}#{player["tagLine"]}")

def save_matches_bronze(spark, api_key):
    super_region_map = {
        "americas": ["na1", "br1", "la1", "la2"],
        "asia": ["kr", "jp1"],
        "europe": ["eun1", "euw1", "me1", "tr1", "ru"],
        "sea": ["oc1", "sg2", "tw2", "vn2"]
    }

    players_df = spark.read.format("delta").load("data/bronze/players")
    players = {}
    for row in players_df.collect():
        players[row["puuid"]] = {
            "gameName": row["gameName"],
            "tagLine": row["tagLine"],
            "lol-region": row["lol-region"]
        }
    
    if not players:
        print("No players found")
        return
    
    match_ids_df = spark.read.format("delta").load("data/bronze/match_ids")
    match_ids = [(row["puuid"], row["matchId"]) for row in match_ids_df.collect()]
    
    if not match_ids:
        print("No match IDs found")
        return

    stored_matches = []
    if os.path.exists("data/bronze/matches") and os.path.isdir("data/bronze/matches"):
        matches_df = spark.read.format("delta").load("data/bronze/matches")
        stored_matches = [(row["puuid"], row["matchId"]) for row in matches_df.collect()]

    new_matches = list(set(match_ids) - set(stored_matches))
    match_data = []
    
    for new_match in new_matches:
        player_region = players[new_match[0]]["lol-region"]
        player_super_region = ""
        for super_region in super_region_map:
            if player_region in super_region_map[super_region]:
                player_super_region = super_region
        
        url = f"https://{player_super_region}.api.riotgames.com/lol/match/v5/matches/{new_match[1]}"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        time.sleep(1.2)

        if response.status_code == 200:
            match_info = response.json()
            match_data.append({
                "puuid": new_match[0],
                "matchId": match_info["metadata"]["matchId"],
                "dataVersion": match_info["metadata"]["dataVersion"],
                "modifiedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        if len(match_data) % 10 == 0:
            print(f"Loaded {len(match_data)}/{len(new_matches)} matches")
    
    if match_data:
        df_match_data = spark.createDataFrame(match_data)
        df_match_data.write.format("delta").mode("append").save("data/bronze/matches")
        matches_by_puuid = df_match_data.groupBy("puuid").count().collect()
        for player in players:
            for row in matches_by_puuid:
                if row.puuid == player:
                    print(f"Saved {row['count']} matches for {players[player][0]}#{players[player][1]}")
    else:
        print("No new matches found")

def add_player():
    game_name, tag_line, api_key = get_user_input()
    spark = init_spark()
    save_player_bronze(spark, game_name, tag_line, api_key)
    save_match_ids_bronze(spark, api_key)
    save_matches_bronze(spark, api_key)
    spark.stop()