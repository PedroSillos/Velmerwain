import getpass
from datetime import datetime
from pyspark.sql import SparkSession
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
        existing = df.filter((df.gameName == game_name) & (df.tagLine == tag_line))
        
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
        player_data = [(data["puuid"], game_name, tag_line, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
        df = spark.createDataFrame(player_data, ["puuid", "gameName", "tagLine", "modifiedOn"])
        df.write.format("delta").mode("append").save(bronze_path)
        print("Player saved")
    else:
        print(f"API Error: {response.status_code}")

def main():
    game_name, tag_line, api_key = get_user_input()
    spark = init_spark()
    save_player_bronze(spark, game_name, tag_line, api_key)
    spark.stop()

if __name__ == "__main__":
    main()