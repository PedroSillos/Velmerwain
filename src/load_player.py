import getpass
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import requests

def get_input():
    game_name = input("Enter game name: ")
    tag_line = input("Enter tag line: ")
    api_key = getpass.getpass("Enter API key: ")
    return game_name, tag_line, api_key

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_player_bronze(spark, game_name, tag_line, api_key):
    # Define all regions and their super regions
    super_region_map = {
        "americas": ["na1", "br1", "la1", "la2"],
        "asia": ["kr", "jp1"],
        "europe": ["eun1", "euw1", "me1", "tr1", "ru"],
        "sea": ["oc1", "sg2", "tw2", "vn2"]
    }

    # Load existing players to avoid re-fetching
    existing_players = set()
    try:
        existing_player_rows = spark.read.format("delta").load("data/bronze/player").collect()
        existing_players = {f"{row["gameName"]}#{row["tagLine"]}".upper() for row in existing_player_rows}
    except:
        print("\nNo player table found")
        
    # If player does not exist, load from 
    if f"{game_name}#{tag_line}".upper() not in existing_players:
        print(f"\nPlayer {game_name}#{tag_line} not found in existing players")

        url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
            
        while response.status_code == 429:
            # If rate limit exceeded, wait and retry
            print("\nRate limit exceeded. Sleeping for 30 seconds...")
            time.sleep(30)
            url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
        if response.status_code == 200:
            account_data = response.json()
            # Save new player to Delta table
            df = spark.createDataFrame(
                [{
                    "gameName": account_data["gameName"],
                    "tagLine": account_data["tagLine"],
                    "puuid": account_data["puuid"]
                }]
            )
            df.write.format("delta").mode("append").save("data/bronze/player")
            print(f"\nSaved account data for {account_data["gameName"]}#{account_data["tagLine"]}")
        else:
            print(f"\nAPI Error: {response.status_code}")
            return
    
    else:
        print(f"\nPlayer {game_name}#{tag_line} already exists in the player table")

        # Update existing player info
        # TBD

def load_player():
    print("\n ***** Start load players ***** \n")
    game_name, tag_line, api_key = get_input()
    spark = init_spark()
    load_player_bronze(spark, game_name, tag_line, api_key)
    spark.stop()
    print("\n ***** End load players ***** \n")