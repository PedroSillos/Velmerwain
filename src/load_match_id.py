import getpass
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import requests
import time

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_match_id_bronze(spark, api_key):
    # Get all players from player table
    new_players = set()
    try:
        existing_player_rows = spark.read.format("delta").load("data/bronze/player").collect()
        new_players = {row["puuid"] for row in existing_player_rows}
    except:
        print("\nNo player table found")
        return
    
    # Get existing players with match_id to avoid re-fetching
    existing_players = set()
    try:
        existing_player_with_match_id_rows = spark.read.format("delta").load("data/bronze/match_id").select("puuid").distinct().collect()
        existing_players = {row["puuid"] for row in existing_player_with_match_id_rows}
    except:
        print("\nNo puuids found in match_id table")
    
    new_players = new_players - existing_players
    
    # For each new player, get their match_ids
    existing_match_ids = set()
    try:
        existing_match_id_rows = spark.read.format("delta").load("data/bronze/match_id").select("matchId").distinct().collect()
        existing_match_ids = {row["matchId"] for row in existing_match_id_rows}
    except:
        print("\nNo matchIds found in match_id table")

    count = 1
    for new_player in new_players:
        new_match_ids = []
        
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{new_player}/ids?queue=420&type=ranked&start=0&count=100"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        # Rate limit exceeded
        while response.status_code == 429:
            print("Rate limit exceeded. Sleeping for 30 seconds...")
            time.sleep(30)
            url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{new_player}/ids?queue=420&type=ranked&start=0&count=100"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
        if response.status_code == 200:
            match_ids = response.json()
            for match_id in match_ids:
                if match_id not in existing_match_ids:
                    new_match_ids.append({"puuid": new_player, "matchId": match_id})
                    existing_match_ids.add(match_id)
        else:
            print(f"\nAPI Error: {response.status_code}")
            return
        
        if new_match_ids:
            df = spark.createDataFrame(new_match_ids)
            df.write.format("delta").mode("append").save("data/bronze/match_id")

        print(f"\nSaved {len(new_match_ids)} match_ids for player {count}/{len(new_players)}")
        count += 1

def load_match_id():
    print("\n ***** Start load match_ids ***** \n")
    api_key = getpass.getpass("API Key: ")
    spark = init_spark()
    load_match_id_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load match_ids ***** \n")