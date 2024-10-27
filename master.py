import os
import sys
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as sum_, avg, round, dayofweek, 
    weekofyear, date_format, to_date, concat_ws
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session for EMR"""
    try:
        spark = SparkSession.builder \
            .appName("NYC Taxi Trip Analysis") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def read_taxi_data(spark, s3_path):
    """Read NYC taxi data from S3"""
    try:
        logger.info(f"Reading data from {s3_path}")
        return spark.read.csv(s3_path, header=True, inferSchema=True)
    except Exception as e:
        logger.error(f"Failed to read data: {str(e)}")
        raise

def clean_data(df):
    """Clean and transform the taxi data"""
    try:
        logger.info("Starting data cleaning process")
        
        # Drop unnecessary columns and duplicates
        df = df.drop('Ehail_fee').dropDuplicates()
        
        # Add date and time columns
        df = df.withColumn("Pickup_date", to_date("lpep_pickup_datetime"))
        df = df.withColumn("Dropoff_date", to_date("Lpep_dropoff_datetime"))
        df = df.withColumn("Pickup_time", date_format("lpep_pickup_datetime", "HH:mm:ss"))
        df = df.withColumn("Dropoff_time", date_format("Lpep_dropoff_datetime", "HH:mm:ss"))
        
        # Add week and day columns
        df = df.withColumn("Pickup_day_of_week", dayofweek("lpep_pickup_datetime"))
        df = df.withColumn("Pickup_week", weekofyear("lpep_pickup_datetime"))
        df = df.withColumn("Dropoff_day_of_week", dayofweek("Lpep_dropoff_datetime"))
        df = df.withColumn("Dropoff_week", weekofyear("Lpep_dropoff_datetime"))
        
        # Drop original datetime columns
        df = df.drop("lpep_pickup_datetime", "Lpep_dropoff_datetime")
        
        logger.info("Data cleaning completed successfully")
        return df
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        raise

def create_fact_table(df):
    """Create the weekly fact table"""
    try:
        logger.info("Creating fact table")
        
        # Filter out zero fares
        df_filtered = df.filter(df.Fare_amount > 0)
        
        # Calculate tip fraction
        df_filtered = df_filtered.withColumn(
            "tip_fraction",
            col("Tip_amount") / col("Fare_amount")
        )
        
        # Calculate average tip for generous customer flag
        avg_tip_amount = df_filtered.agg(avg("Tip_amount").alias("avg_tip_amount")).collect()[0][0]
        df_filtered = df_filtered.withColumn(
            "Generous_customer_flg",
            col("Tip_amount") > avg_tip_amount
        )
        
        # Create weekly aggregations
        fact_table = df_filtered.groupBy(
            "VendorID", "Trip_type ", "Payment_type", "Pickup_week"
        ).agg(
            sum_("Trip_distance").alias("Total_trip_distance"),
            avg("Trip_distance").alias("Avg_trip_distance"),
            sum_("Fare_amount").alias("Total_fare_amount"),
            avg("Fare_amount").alias("Avg_fare_amount"),
            sum_("Total_amount").alias("Total_total_amount"),
            sum_("Extra").alias("Total_extra"),
            avg("Extra").alias("Avg_extra"),
            sum_("MTA_tax").alias("Total_mta_tax"),
            avg("Tip_amount").alias("Avg_tip_amount"),
            avg("improvement_surcharge").alias("Avg_improvement_surcharge"),
            sum_(col("Generous_customer_flg").cast("int")).alias("Generous_customer_count")
        )
        
        # Add Lucky flag
        avg_weekly_generous = fact_table.agg(
            avg("Generous_customer_count").alias("avg_weekly_generous_count")
        ).collect()[0][0]
        
        fact_table = fact_table.withColumn(
            "Lucky_flg",
            col("Generous_customer_count") > avg_weekly_generous
        )
        
        logger.info("Fact table created successfully")
        return fact_table
    except Exception as e:
        logger.error(f"Failed to create fact table: {str(e)}")
        raise

def save_to_s3(df, output_path):
    """Save DataFrame to S3"""
    try:
        logger.info(f"Saving data to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        logger.info("Data saved successfully")
    except Exception as e:
        logger.error(f"Failed to save data: {str(e)}")
        raise

def main():
    """Main execution function"""
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Define paths
        input_path = "s3://weclouddata/datasets/transformation/nyc_taxi_data/data/green_tripdata_2015-*.csv"
        output_path = "s3a://emr-transformations/fact_table/"
        
        # Execute pipeline
        raw_df = read_taxi_data(spark, input_path)
        cleaned_df = clean_data(raw_df)
        fact_table_df = create_fact_table(cleaned_df)
        save_to_s3(fact_table_df, output_path)
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
