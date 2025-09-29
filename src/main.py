import os
import getpass
import time
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

        stored_gameName = existing.select("gameName").collect()[0]["gameName"]
        stored_tagLine = existing.select("tagLine").collect()[0]["tagLine"]
        
        if existing.count() > 0:
            # Update modifiedOn
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
    
    # Fetch from API
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    response = requests.get(url, headers={"X-Riot-Token": api_key})
    
    if response.status_code == 200:
        data = response.json()
        player_data = [(data["puuid"], data["gameName"], data["tagLine"], datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
        df = spark.createDataFrame(player_data, ["puuid", "gameName", "tagLine", "modifiedOn"])
        df.write.format("delta").mode("append").save(bronze_path)
        print(f"Player {data["gameName"]}#{data["tagLine"]} saved")
    else:
        print(f"API Error: {response.status_code}")

def save_match_ids_bronze(spark, api_key):
    players_df = spark.read.format("delta").load("data/bronze/players")
    players = [(row.puuid, row.gameName, row.tagLine) for row in players_df.collect()]
    
    if not players:
        print("No players found")
        return
    
    match_ids_path = "data/bronze/match_ids"
    match_id_data = []
    
    for player in players:
        start = 0
        match_ids = ['dummy'] # we need a non-empty list to enter the loop
        while match_ids != []:
            url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{player[0]}/ids?queue=420&type=ranked&start={start}&count=100"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
            
            if response.status_code == 200:
                match_ids = response.json()
                for match_id in match_ids:
                    match_id_data.append(
                        {
                            "puuid": player[0],
                            "matchId": match_id,
                            "modifiedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    )
                start += 100
    
    if match_id_data:
        df_new_match_id_data = spark.createDataFrame(match_id_data)
        df_new_match_id_data.write.format("delta").mode("overwrite").save(match_ids_path)
        match_ids_by_puuid = df_new_match_id_data.groupBy("puuid").count().collect()
        for player in players:
            for row in match_ids_by_puuid:
                if row.puuid == player[0]:
                    print(f"Saved {row['count']} match IDs for {player[1]}#{player[2]}")

def save_matches_bronze(spark, api_key):
    players_df = spark.read.format("delta").load("data/bronze/players")
    players = {}
    for row in players_df.collect():
        if not row.puuid in players:
            players[row.puuid] = []
        players[row.puuid] = (row.gameName, row.tagLine)
    
    if not players:
        print("No players found")
        return
    
    match_ids_df = spark.read.format("delta").load("data/bronze/match_ids")
    match_ids = []
    for row in match_ids_df.collect():
        match_ids.append((row.puuid, row.matchId))
    
    if not match_ids:
        print("No match IDs found")
        return

    stored_matches = []
    if os.path.exists("data/bronze/matches") and os.path.isdir("data/bronze/matches"):
        matches_df = spark.read.format("delta").load("data/bronze/matches")
        for row in matches_df.collect():
            stored_matches.append((row.puuid, row.matchId))

    new_matches = list(set(match_ids) - set(stored_matches))
    
    match_data = []
    
    for new_match in new_matches:
        url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{new_match[1]}"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        time.sleep(1.2) # The rate limit for a personal keys is 100 requests every 2 minutes

        if response.status_code == 200:
            match_info = response.json()
            match_data.append(
                {
                    "puuid": new_match[0],
                    "matchId": match_info["metadata"]["matchId"],
                    "dataVersion": match_info["metadata"]["dataVersion"],
                    "modifiedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            )
        
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
        

def display_players(spark):
    try:
        df = spark.read.format("delta").load("data/bronze/players")
        players = df.collect()
        
        if not players:
            print("No players found")
            return
            
        print("\nPlayers stored:")
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
            print(f"{player.gameName}#{player.tagLine}: {count} match ids")
    except:
        print("\nNo match IDs found")

def display_matches(spark):
    try:
        players_df = spark.read.format("delta").load("data/bronze/players")
        match_df = spark.read.format("delta").load("data/bronze/matches")
        
        players = players_df.collect()
        match_count = match_df.groupBy("puuid").count().collect()
        match_dict = {row["puuid"]: row["count"] for row in match_count}
        
        print("\nMatches stored:")
        for player in players:
            count = match_dict.get(player.puuid, 0)
            print(f"{player.gameName}#{player.tagLine}: {count} matches")
    except:
        print("\nNo matches found")

def main():
    action = input("Enter 'add' to add player or 'list' to show all players: ")
    
    if action.upper() in ["ADD", "AD", "A"]:
        game_name, tag_line, api_key = get_user_input()
        spark = init_spark()
        save_player_bronze(spark, game_name, tag_line, api_key)
        save_match_ids_bronze(spark, api_key)
        save_matches_bronze(spark, api_key)
        spark.stop()
    
    else:
        spark = init_spark()
        display_players(spark)
        display_match_ids(spark)
        display_matches(spark)
        spark.stop()

if __name__ == "__main__":
    main()