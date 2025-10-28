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
    players = set()
    try:
        player_rows = spark.read.format("delta").load("data/bronze/player").collect()
        players = {row["puuid"] for row in player_rows}
    except:
        print("\nNo player table found")
        return

    print(f"\nFound {len(players)} players")
    
    if not players:
        print("\nNo players found")
        return
    
    # Get existing match_ids
    existing_match_ids = set()
    try:
        existing_match_id_rows = spark.read.format("delta").load("data/bronze/match_id").collect()
        existing_match_ids = {row["matchId"] for row in existing_match_id_rows}
    except:
        print("\nNo match_id table found")
    
    # Counter to know how many players were loaded
    count = 1
    # Empty list to hold new match IDs
    new_match_ids = []
    # Only fetch match IDs for new players
    for player in players:
        print(f"\nStart fetching match_ids for player {count}/{len(players)}")
        # Initialize to enter the loop
        match_ids = ["text because cannot be empty"]
        start = 0
        while not match_ids == []:
            # Fetch match IDs for player from Riot API
            url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{player}/ids?queue=420&type=ranked&start={start}&count=100"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
            # Check if rate limit was exceeded
            while response.status_code == 429:
                # If rate limit exceeded, wait and retry
                print("Rate limit exceeded. Sleeping for 30 seconds...")
                time.sleep(30)
                url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{player}/ids?queue=420&type=ranked&start={start}&count=100"
                response = requests.get(url, headers={"X-Riot-Token": api_key})
            # If successful, load the match IDs
            if response.status_code == 200:
                match_ids = response.json()
                for match_id in match_ids:
                    # Check if match_ids are not already in match_id table
                    if match_id not in existing_match_ids:
                        new_match_ids.append({"puuid": player, "matchId": match_id})
                        existing_match_ids.add(match_id)
            else:
                print(f"\nAPI Error: {response.status_code} for player {player}")
                break
            # Increment start for pagination
            start += 100
        print(f"\nFinished fetching match_ids for player {count}/{len(players)}")
        # Save new match IDs to match_id table
        # Save every 10 players to reduce number of writes (but also save at the end)
        if new_match_ids and (count % 10 == 0 or count == len(players)):
            df = spark.createDataFrame(new_match_ids)
            df.write.format("delta").mode("append").save("data/bronze/match_id")
            # Print progress
            print(f"\nSaved {len(new_match_ids)} match_ids for players until {count}/{len(players)}")
            # Reset new_match_ids list
            new_match_ids = []
        if (not new_match_ids) and (count % 10 == 0 or count == len(players)):
            print(f"\nNo new match_ids to save for {len(players)} players")
        # Increment player counter
        count += 1

def load_match_id():
    print("\n ***** Start load match_ids ***** \n")
    api_key = getpass.getpass("API Key: ")
    print("\n")
    spark = init_spark()
    load_match_id_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load match_ids ***** \n")