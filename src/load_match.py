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

def load_match_bronze(spark, api_key):
    # Get all match_ids from match_id table
    source_match_ids = set()
    try:
        source_match_id_rows = spark.read.format("delta").load("data/bronze/match_id").select("matchId").distinct().collect()
        source_match_ids = {row["matchId"] for row in source_match_id_rows}
    except:
        print("\nNo match_id table found")

    # Stop if there are no match_ids
    if not source_match_ids:
        print("\nNo match_ids found in match_id table")
        return

    # Get all match_ids from match table
    destination_match_ids = set()
    try:
        destination_match_id_rows = spark.read.format("delta").load("data/bronze/match").select("matchId").distinct().collect()
        destination_match_ids = {row["matchId"] for row in destination_match_id_rows}
    except:
        print("\nNo match table found")
    
    # Get new match_ids
    new_match_ids = source_match_ids - destination_match_ids

    print(f"\nFound {len(new_match_ids)} new match_ids: ", new_match_ids.pop())
    
    """
    # Counter to know how many players were loaded
    count = 1
    # Empty list to hold new match IDs
    new_match_ids = []
    # Only fetch match IDs for new players
    for new_player in new_players:
        # Fetch match IDs for new_player from Riot API
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{new_player}/ids?queue=420&type=ranked&start=0&count=100"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        # Check if rate limit was exceeded
        while response.status_code == 429:
            # If rate limit exceeded, wait and retry
            print("Rate limit exceeded. Sleeping for 30 seconds...")
            time.sleep(30)
            url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{new_player}/ids?queue=420&type=ranked&start=0&count=100"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
        # If successful, load the match IDs
        if response.status_code == 200:
            match_ids = response.json()
            for match_id in match_ids:
                # Check if match_ids are not already in match_id table
                if match_id not in existing_match_ids:
                    new_match_ids.append({"puuid": new_player, "matchId": match_id})
                    existing_match_ids.add(match_id)
        else:
            print(f"\nAPI Error: {response.status_code}")
            return
        # Save new match IDs to match_id table
        # Save every 30 players to reduce number of writes (but also save at the end)
        if new_match_ids and (count % 30 == 0 or count == len(new_players)):
            df = spark.createDataFrame(new_match_ids)
            df.write.format("delta").mode("append").save("data/bronze/match_id")
            # Print progress
            print(f"\nSaved {len(new_match_ids)} match_ids for players until {count}/{len(new_players)}")
            # Reset new_match_ids list
            new_match_ids = []
        # Increment player counter
        count += 1
    """

def load_match():
    print("\n ***** Start load matches ***** \n")
    api_key = getpass.getpass("API Key: ")
    spark = init_spark()
    load_match_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load matches ***** \n")