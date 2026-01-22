import pandas as pd
import pymongo
import pyodbc
from dagster import asset, Definitions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# --- Configuration ---
MONGO_CONNECTION_STRING = "mongodb://mongo_db:27017"
SQL_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql_server;"
    "UID=sa;"
    "PWD=MyComplexPassword123!;"
    "TrustServerCertificate=yes;"
)


def get_spark_session():
    """Initialize and return a SparkSession with required connectors."""
    # Using Connector 10.4.0 for Spark 3.5 compatibility
    return SparkSession.builder \
        .appName("RedSeaPipeline") \
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,"
            "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"
        ) \
        .master("local[*]") \
        .getOrCreate()


# --- ASSET 1: Ingest REAL Mined News (From API Miner) ---
@asset
def ingest_news_to_mongo():
    """Ingest historical news data from CSV into MongoDB."""
    client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    db = client["SupplyChainDB"]
    collection = db["raw_gdelt_events"]

    collection.delete_many({})  # Clear old data

    # POINT TO YOUR MINER OUTPUT (Real Data)
    csv_path = "/opt/dagster/app/jobs/historical_news_raw.csv"

    try:
        print(f"Reading Real News from: {csv_path}")
        df = pd.read_csv(csv_path)
        records = df.to_dict("records")
        if records:
            collection.insert_many(records)
            return f"Success! Inserted {len(records)} Real News events."
        return "CSV was empty."
    except Exception as e:
        return f"Error: {e}"


# --- ASSET 2: Ingest UPSAMPLED Rates (Hourly Resolution) ---
@asset
def ingest_rates_to_sql():
    """Ingest upsampled shipping rates from CSV into SQL Server."""
    df = pd.read_csv("/opt/dagster/app/jobs/upsampled_rates.csv")

    df['Date'] = pd.to_datetime(df['Date'])

    conn = pyodbc.connect(SQL_CONN_STR)
    cursor = conn.cursor()

    cursor.execute("""
        IF EXISTS (
            SELECT * FROM sysobjects
            WHERE name='Staging_Rates' and xtype='U'
        )
        DROP TABLE Staging_Rates
    """)
    cursor.execute("""
        CREATE TABLE Staging_Rates (
            Date DATETIME,
            Route NVARCHAR(100),
            Price FLOAT
        )
    """)

    # Fast Insert
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO Staging_Rates (Date, Route, Price) VALUES (?, ?, ?)",
            row['Date'],
            row['Route'],
            row['Price']
        )
    conn.commit()
    return f"Uploaded {len(df)} Hourly Rate records."


# --- ASSET 3: SPARK TRANSFORMATION (Fixed Date Parsing) ---
@asset(deps=[ingest_news_to_mongo, ingest_rates_to_sql])
def transform_data_with_spark():
    """
    Perform ETL using Spark:
    1. Read news from Mongo (Daily Aggregation)
    2. Read rates from SQL (Hourly)
    3. Join datasets
    4. Write 'Gold' table back to SQL
    """
    spark = get_spark_session()

    # 1. READ NEWS
    # FIX: Change 'Day' to StringType because Mongo has "20231019" as text
    news_schema = StructType([
        StructField("GlobalEventID", IntegerType(), True),
        StructField("Day", StringType(), True),  # CHANGED TO STRING
        StructField("Country", StringType(), True),
        StructField("Tone", DoubleType(), True),
        StructField("SourceURL", StringType(), True)
    ])

    df_news = spark.read.format("mongodb") \
        .schema(news_schema) \
        .option("connection.uri", MONGO_CONNECTION_STRING) \
        .option("database", "SupplyChainDB") \
        .option("collection", "raw_gdelt_events") \
        .load()

    # Data Cleaning: Handle potential "T" in dates or pure integers
    # We take the first 8 characters to get YYYYMMDD
    from pyspark.sql.functions import substring

    df_news = df_news.withColumn("day_str", substring(col("Day"), 1, 8)) \
        .withColumn("event_date", to_date(col("day_str"), "yyyyMMdd")) \
        .withColumn("conflict_intensity", col("Tone") * -1)

    # Aggregate News by Day
    df_daily_news = df_news.groupBy("event_date") \
        .agg(
            count("*").alias("news_count"),
            avg("conflict_intensity").alias("avg_conflict_score")
        )

    # 2. READ RATES (Hourly Resolution)
    jdbc_url = (
        "jdbc:sqlserver://sql_server:1433;"
        "databaseName=master;"
        "encrypt=true;"
        "trustServerCertificate=true;"
    )

    df_rates = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "Staging_Rates") \
        .option("user", "sa") \
        .option("password", "MyComplexPassword123!") \
        .option(
            "driver",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        ) \
        .load()

    # 3. JOIN STRATEGY
    # We join Hourly Rates to Daily News.
    df_final = df_rates.join(
        df_daily_news,
        to_date(df_rates.Date) == df_daily_news.event_date,
        how="inner"
    ).select(
        df_rates.Date.alias("full_date"),  
        df_rates.Price,
        df_daily_news.news_count,
        df_daily_news.avg_conflict_score
    )

    # 4. WRITE GOLD TABLE
    msg = "Spark Job Ran. No matching dates found between News and Rates."

    if df_final.count() > 0:
        df_final.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "Gold_Analytics_RedSea") \
            .option("user", "sa") \
            .option("password", "MyComplexPassword123!") \
            .option(
                "driver",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            ) \
            .mode("overwrite") \
            .save()
        msg = f"Success! Saved {df_final.count()} rows (Hourly Rates + News)."

    spark.stop()
    return msg


defs = Definitions(
    assets=[
        ingest_news_to_mongo,
        ingest_rates_to_sql,
        transform_data_with_spark
    ]

)
