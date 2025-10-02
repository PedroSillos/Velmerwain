import getpass
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import requests

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_player_bronze(spark, api_key):
    regions = ["na1", "br1", "la1", "la2", "kr", "jp1", "eun1", "euw1", "me1", "tr1", "ru", "oc1", "sg2", "tw2", "vn2"]
    super_region_map = {
        "americas": ["na1", "br1", "la1", "la2"],
        "asia": ["kr", "jp1"],
        "europe": ["eun1", "euw1", "me1", "tr1", "ru"],
        "sea": ["oc1", "sg2", "tw2", "vn2"]
    }
    
    existing_players = set()
    
    try:
        existing_player_rows = spark.read.format("delta").load("data/bronze/player").collect()
        existing_players = {row["puuid"] for row in existing_player_rows}
    except:
        print("No player table found")

    new_players = []
    
    for region in regions:
        for super_region_i in super_region_map:
            if region in super_region_map[super_region_i]:
                super_region = super_region_i
        
        # There are too many players, so we limit to only KR region for now
        if region != "kr":
            continue
        
        url = f"https://{region}.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        
        if response.status_code == 200:
            league_data = response.json()
            for league_entry in league_data["entries"]:
                if league_entry["puuid"] not in existing_players:
                    new_players.append({"puuid": league_entry["puuid"]})
        else:
            print(f"API Error: {response.status_code}")
            return

        url = f"https://{region}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        
        if response.status_code == 200:
            league_data = response.json()
            for league_entry in league_data["entries"]:
                if league_entry["puuid"] not in existing_players:
                    new_players.append({"puuid": league_entry["puuid"]})
        else:
            print(f"API Error: {response.status_code}")
            return

        url = f"https://{region}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        
        if response.status_code == 200:
            league_data = response.json()
            for league_entry in league_data["entries"]:
                if league_entry["puuid"] not in existing_players:
                    new_players.append({"puuid": league_entry["puuid"]})
        else:
            print(f"API Error: {response.status_code}")
            return
        
        if new_players:
            df = spark.createDataFrame(new_players)
            df.write.format("delta").mode("append").save("data/bronze/player")

        print(f"\nSaved {len(new_players)} players from {region}")

def load_player():
    print("\n ***** Start load players ***** \n")
    api_key = getpass.getpass("API Key: ")
    spark = init_spark()
    load_player_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load players ***** \n")