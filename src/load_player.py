import getpass
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import requests
from datetime import datetime
import time

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
    tiers = ["IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER"]
    divisions = []
    
    player_data = []
    
    for region in regions:

        player_super_region = ""
        
        for super_region in super_region_map:
            if region in super_region_map[super_region]:
                player_super_region = super_region

        for tier in tiers:
            # These tiers only have one division
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                divisions = ["I"]
            else:
                divisions = ["IV", "III", "II", "I"]
            
            for division in divisions:
                league_data = []
                page_number = 1
                count = 0

                print(f"{region} - {tier} - {division} - {page_number} - {count}")
                
                # while will break if league_data == []
                while True:
                    url = f"https://{region}.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page_number}"
                    response = requests.get(url, headers={"X-Riot-Token": api_key})
                    
                    if response.status_code == 200:
                        league_data = response.json()
                        if league_data == []:
                            break
                        
                        for league_entry in league_data:
                            player_data.append(
                                {
                                    "puuid": league_entry["puuid"],
                                    "gameName": None,
                                    "tagLine": None,
                                    "lolRegion": region,
                                    "lolSuperRegion": player_super_region,
                                    "modifiedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                }
                            )
                        
                        page_number += 1
                        count += len(league_data)

                        print(f"{region} - {tier} - {division} - {page_number} - {count}")
                    
                    else:
                        if response.status_code == 429:
                            print("Rate limit exceeded, sleeping for 30 seconds...")
                            time.sleep(30)
                            continue
                        else:
                            print(f"API Error: {response.status_code}")
                            return
                        
                print(f"\n ***** Wrote {region} - {tier} - {division} - {count} ***** \n")
                df = spark.createDataFrame(player_data)
                df.write.format("delta").mode("append").save("data/bronze/players")

def load_player():
    api_key = getpass.getpass("\nAPI Key: ")
    print("\n ***** Start load player ***** \n")
    spark = init_spark()
    load_player_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load player ***** \n")