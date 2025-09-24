import getpass
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as SparkFunctions
from delta import configure_spark_with_delta_pip
import requests

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
    bronze_path = "data/bronze/players"
    
    # Check if player exists
    try:
        df = spark.read.format("delta").load(bronze_path)
        existing = df.filter(
            (SparkFunctions.upper(df.gameName) == game_name.upper())
            & (SparkFunctions.upper(df.tagLine) == tag_line.upper())
        )
        
        if existing.count() > 0:
            # Update modifiedOn
            df.createOrReplaceTempView("players")
            spark.sql(f"""
                UPDATE players 
                SET modifiedOn = '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' 
                WHERE gameName = '{game_name}' AND tagLine = '{tag_line}'
            """)
            print("Player updated")
            return
    except:
        pass
    
    # Fetch from API
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    response = requests.get(url, headers={"X-Riot-Token": api_key})
    
    if response.status_code == 200:
        data = response.json()
        player_data = [(data["puuid"], data["gameName"], data["tagLine"], datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
        df = spark.createDataFrame(player_data, ["puuid", "gameName", "tagLine", "modifiedOn"])
        df.write.format("delta").mode("append").save(bronze_path)
        print("Player saved")
    else:
        print(f"API Error: {response.status_code}")

def save_match_ids_bronze(spark, api_key):
    players_df = spark.read.format("delta").load("data/bronze/players")
    puuids = [row.puuid for row in players_df.collect()]
    
    if not puuids:
        print("No players found")
        return
    
    match_ids_path = "data/bronze/match_ids"
    match_id_data = []
    
    for puuid in puuids:
        start = 0
        match_ids = ['dummy'] # we need a non-empty list to enter the loop
        while match_ids != []:
            url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&type=ranked&start={start}&count=100"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
            
            if response.status_code == 200:
                match_ids = response.json()
                for match_id in match_ids:
                    match_id_data.append((puuid, match_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                start += 100
    
    if match_id_data:
        df = spark.createDataFrame(match_id_data, ["puuid", "matchId", "modifiedOn"])
        df.write.format("delta").mode("overwrite").save(match_ids_path)
        print(f"Saved {len(match_id_data)} match records")

def display_players(spark):
    try:
        df = spark.read.format("delta").load("data/bronze/players")
        players = df.collect()
        
        if not players:
            print("No players found")
            return
            
        print(f"{'Game Name':<20} {'Tag Line':<10} {'Puuid':<80} {'Modified On':<20}")
        print("-" * 135)
        for player in players:
            print(f"{player.gameName:<20} {player.tagLine:<10} {player.puuid:<80} {player.modifiedOn:<20}")
    except:
        print("No players found")

def display_match_ids(spark):
    try:
        players_df = spark.read.format("delta").load("data/bronze/players")
        match_df = spark.read.format("delta").load("data/bronze/match_ids")
        
        players = players_df.collect()
        match_count = match_df.groupBy("puuid").count().collect()
        match_dict = {row["puuid"]: row["count"] for row in match_count}
        
        print("\nMatch IDs stored:")
        for player in players:
            count = match_dict.get(player.puuid, 0)
            print(f"{player.gameName}#{player.tagLine}: {count} matches")
    except:
        print("\nNo match IDs found")

def main():
    action = input("Enter 'add' to add player or 'list' to show all players: ")
    
    if action.upper() == "ADD":
        game_name, tag_line, api_key = get_user_input()
        spark = init_spark()
        save_player_bronze(spark, game_name, tag_line, api_key)
        save_match_ids_bronze(spark, api_key)
        spark.stop()
    
    else:
        spark = init_spark()
        display_players(spark)
        display_match_ids(spark)
        spark.stop()

if __name__ == "__main__":
    main()