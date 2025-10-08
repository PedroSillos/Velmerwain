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

    print(f"\nFound {len(new_match_ids)} new match_ids")
    
    # Counter to know how many matches were loaded
    count = 1
    # Empty list to hold new matches
    new_matches = []
    # Only fetch matches for new match ids
    for new_match_id in new_match_ids:
        # Fetch match for new_match_id from Riot API
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{new_match_id}"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        # Check if rate limit was exceeded
        while response.status_code == 429:
            # If rate limit exceeded, wait and retry
            print("Rate limit exceeded. Sleeping for 30 seconds...")
            time.sleep(30)
            url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{new_match_id}"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
        # If successful, load the match
        if response.status_code == 200:
            match_api_data = response.json()
            # Get participants and iterate (there will be a row in bronze/match for every participant)
            participants = match_api_data["info"]["participants"]
            for participant in participants:
                participant_match = {
                    "matchId": match_api_data["metadata"]["matchId"],
                    "gameDuration": match_api_data["info"]["gameDuration"],
                    "gameVersion": '.'.join(match_api_data["info"]["gameVersion"].split('.')[:2]),
                    "assists": participant["assists"],
                    "baronKills": participant["baronKills"],
                    "champLevel": participant["champLevel"]
                }
                new_matches.append(participant_match)
        else:
            print(f"\nAPI Error: {response.status_code}")
            return
        # Save new matches to match table
        # Save every 30 matches to reduce number of writes (but also save at the end)
        if new_matches and (count % 30 == 0 or count == len(new_match_ids)):
            df = spark.createDataFrame(new_matches)
            df.write.format("delta").mode("append").save("data/bronze/match")
            # Print progress
            print(f"\nSaved {len(new_matches)} matches for match_ids until {count}/{len(new_match_ids)}")
            # Reset new_match_ids list
            new_matches = []
        # Increment player counter
        count += 1

def load_match():
    print("\n ***** Start load matches ***** \n")
    api_key = getpass.getpass("API Key: ")
    spark = init_spark()
    load_match_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load matches ***** \n")