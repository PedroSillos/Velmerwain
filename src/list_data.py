from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

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

def list_data():
    spark = init_spark()
    display_players(spark)
    display_match_ids(spark)
    display_matches(spark)
    spark.stop()