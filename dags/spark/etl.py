import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, DateType
from pyspark.sql.functions import datediff, to_date, concat, col, date_format, to_timestamp


def create_spark_session():
    """
    Initialises spark session
    :return: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .appName("Deutsche-Boerse Data Ingestion") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.entrypoint", "s3-eu-central-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("com.amazonaws.services.s3.enableV4", True) \
        .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
        .getOrCreate()
    return spark


def process_xetra_data(spark, output_uri):
    """
    Processes csv XETRA German Electronic Exchange (Xetra) data onto parquet table in s3
    :param spark: SparkSession object
    :param input_uri: s3 input uri
    :param output_uri: s3 output uri
    """
    # XETRA data schema
    xetra_schema = StructType([
        StructField("isin", StringType(), False),
        StructField("mnemonic", StringType(), False),
        StructField("security_description", StringType(), False),
        StructField("security_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("security_id", StringType(), False),
        StructField("trading_date", DateType(), False),
        StructField("trading_time", StringType(), False),
        StructField("start_price", DoubleType(), False),
        StructField("max_price", DoubleType(), False),
        StructField("min_price", DoubleType(), False),
        StructField("end_price", DoubleType(), False),
        StructField("traded_volume", LongType(), False),
        StructField("number_of_trades", LongType(), False),
    ])
    # get filepath to csv files
    xetra_path = "s3a://deutsche-boerse-xetra-pds/*/*.csv"

    # read asset price data files
    df = spark.read.option("header", "true").schema(xetra_schema).csv(xetra_path)

    # add trading timestamp to dataframe
    df = df.withColumn(
        "trading_ts",
        date_format(to_timestamp(concat("trading_date", "trading_time"), "yyyy-MM-ddHH:mm"), "yyyy-MM-dd HH:mm:ss")
    )

    # write artists table to parquet files
    df.write.partitionBy("trading_date").parquet(output_uri + "data/xetra")


def process_eurex_data(spark, output_uri):
    """
    Processes csv Eurex Exchange data onto parquet table in s3
    :param spark: SparkSession object
    :param input_uri: s3 input uri
    :param output_uri: s3 output uri
    """
    # EUREX data schema
    eurex_schema = StructType([
        StructField("isin", StringType(), False),
        StructField("market_segment", StringType(), False),
        StructField("underlying_symbol", StringType(), False),
        StructField("underlying_isin", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("security_type", StringType(), False),
        StructField("maturity_string", StringType(), False),
        StructField("strike_price", DoubleType(), False),
        StructField("put_or_call", StringType(), False),
        StructField("mleg", StringType(), False),
        StructField("contract_generation_number", IntegerType(), False),
        StructField("security_id", StringType(), False),
        StructField("trading_date", DateType(), False),
        StructField("trading_time", StringType(), False),
        StructField("start_price", DoubleType(), False),
        StructField("max_price", DoubleType(), False),
        StructField("min_price", DoubleType(), False),
        StructField("end_price", DoubleType(), False),
        StructField("number_of_contracts", LongType(), False),
        StructField("number_of_trades", LongType(), False),
    ])
    # get filepath to csv files
    eurex_path = "s3a://deutsche-boerse-eurex-pds/*/*.csv"

    # read asset price data files
    df = spark.read.option("header", "true").schema(eurex_schema).csv(eurex_path)

    # Create a trading timestamp out of trading_date and trading_time
    df = df.withColumn(
        "trading_ts",
        date_format(to_timestamp(concat("trading_date", "trading_time"), "yyyy-MM-ddHH:mm"), "yyyy-MM-dd HH:mm:ss")
    )

    # Create maturity_date column out of maturity_string
    df = df.withColumn(
        "maturity_date",
        date_format(to_date(col("maturity_string"), "yyyyMMdd"), "yyyy-MM-dd")
    )

    # Create maturity_days column out of maturity_date and trading_date
    df = df.withColumn(
        "maturity_days",
        datediff(
            to_date("maturity_date", "yyyy-MM-dd"),
            to_date("trading_date", "yyyy-MM-dd")
        )
    )

    # Create maturity_months column out of maturity_date and date
    df = df.withColumn(
        "maturity_months",
        (col("maturity_days") / 30.5)
    )

    # Add dimension table with product specification to enhance future querying experience
    dimension_schema = StructType([
        StructField("market_segment", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("product_isin", StringType(), False),
        StructField("product_line", StringType(), False),
        StructField("product_type", StringType(), False),
        StructField("product_type_symbol", StringType(), False),
        StructField("liquidity_class", StringType(), False),
        StructField("trading_environment", StringType(), False),
        StructField("partition", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("us_approval_type", StringType(), False),
        StructField("settlement_type", StringType(), False),
        StructField("contract_size", LongType(), False),
        StructField("tick_size", DoubleType(), False),
        StructField("tick_value", DoubleType(), False),
        StructField("max_order_qty_tsl", LongType(), False),
        StructField("max_tes_qty_tsl", LongType(), False),
        StructField("max_future_spread_qty_tsl", LongType(), False),
        StructField("max_market_order_qty", LongType(), False),
        StructField("position_limit", LongType(), False),
        StructField("pre_trade_limits", StringType(), False),
        StructField("underlying", StringType(), False),
        StructField("underlying_isin", StringType(), False),
        StructField("underlying_name", StringType(), False),
        StructField("underlying_category", StringType(), False),
    ])

    dm_table_path = f"{output_uri}dimension_table/eurex_product_specification.csv"
    dm_table = spark.read.option("header", "true").schema(dimension_schema).csv(dm_table_path)

    df = df.join(
        dm_table[["market_segment", "product_name", "product_type", "underlying_name", "underlying_category"]],
        "market_segment",
        "left"
    )

    # data quality check
    missing_isin = df.select(["market_segment", "mleg"]).where("isin is null").dropDuplicates()
    missing_underlying = df.select(["market_segment", "mleg"]).where("underlying_symbol is null").dropDuplicates()

    # write artists table to parquet files
    df.write.partitionBy("trading_date").parquet(output_uri + "data/eurex")

    # write data quality checks to parquet files
    missing_isin.write.partitionBy("market_segment").parquet(output_uri + "quality_check/missing_isin")
    missing_underlying.write.partitionBy("market_segment").parquet(output_uri + "quality_check/missing_underlying")


def main():
    """
    Runs etl pipeline
    """
    spark = create_spark_session()
    output_data = F"s3a://{os.environ['OUTPUT_URI']}/"

    process_xetra_data(spark, output_data)
    process_eurex_data(spark, output_data)


if __name__ == "__main__":
    main()
