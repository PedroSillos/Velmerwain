from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def display_player(spark):
    try:
        df = spark.read.format("delta").load("data/bronze/player")
        if not df:
            print("0 players found")
            return
        print("\nPlayers stored:",df.count())
        df.show(truncate=False)
    except:
        print("No player table found")

def display_match_id(spark):
    try:
        df = spark.read.format("delta").load("data/bronze/match_id")
        if not df:
            print("0 match_ids found")
            return
        print("\nMatch_id rows:",df.count())
        print("\nDistinct puuids:",df.select("puuid").distinct().count())
        print("\nDistinct matchIds:",df.select("matchId").distinct().count())
        df.show(truncate=False)
    except:
        print("\nNo match_id table found")

def display_match(spark):
    try:
        df = spark.read.format("delta").load("data/bronze/match")
        if not df:
            print("0 matches found")
            return
        print("\nMatches stored:",df.count())
        df.show(truncate=False)
    except:
        print("\nNo match table found")

def list_data():
    print("\n ***** Start list players ***** \n")
    spark = init_spark()
    display_player(spark)
    display_match_id(spark)
    display_match(spark)
    spark.stop()
    print("\n ***** End list players ***** \n")