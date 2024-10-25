from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os

def initialize_spark():
    """Initialize Spark session with Fabric configurations"""
    spark = SparkSession.builder \
        .appName("Fabric CSV Loader") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def get_latest_csv(spark, lakehouse_path):
    """
    Find the latest CSV file in the Fabric Lakehouse directory
    Returns: tuple (latest_file_path, file_name)
    """
    try:
        # List all files in the directory
        file_list = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        ).listStatus(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(lakehouse_path)
        )
        
        # Filter for CSV files and get the latest one
        csv_files = [
            (f.getPath().toString(), f.getModificationTime(), f.getPath().getName())
            for f in file_list
            if f.getPath().getName().endswith('.csv')
        ]
        
        if not csv_files:
            raise Exception("No CSV files found in the specified path")
            
        # Sort by modification time and get the latest
        latest_file = sorted(csv_files, key=lambda x: x[1], reverse=True)[0]
        return latest_file[0], latest_file[2]  # Return full path and filename
        
    except Exception as e:
        raise Exception(f"Error finding latest CSV: {str(e)}")

def load_latest_csv_to_spark(spark, lakehouse_path):
    """
    Load the latest CSV file from Fabric Lakehouse into a Spark DataFrame
    """
    try:
        # Get the latest CSV file
        latest_file_path, file_name = get_latest_csv(spark, lakehouse_path)
        print(f"Loading file: {file_name}")
        
        # Read CSV into Spark DataFrame
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(latest_file_path)
        
        return df
    
    except Exception as e:
        print(f"Error loading CSV to Spark DataFrame: {str(e)}")
        return None

def write_to_delta_table(df, table_path, mode="append"):
    """
    Write DataFrame to Delta table
    
    Parameters:
    - df: Spark DataFrame to write
    - table_path: Path where the Delta table should be stored
    - mode: Write mode ("append", "overwrite", "error", "ignore")
    """
    try:
        df.write \
            .format("delta") \
            .mode(mode) \
            .save(table_path)
        print(f"Successfully wrote data to Delta table at: {table_path}")
        
        # Register table in Spark catalog for SQL queries
        table_name = table_path.split('/')[-1]  # Get table name from path
        df.createOrReplaceTempView(table_name)
        print(f"Table registered as: {table_name}")
        
    except Exception as e:
        print(f"Error writing to Delta table: {str(e)}")

def main():
    """Main execution function"""
    # Fabric Lakehouse paths
    lakehouse_path = "abfss://6xxxxxxxxxxx@onelake.dfs.fabric.microsoft.com/xxxxxx/Files/"
    delta_table_path = "abfss://xxxxxx@onelake.dfs.fabric.microsoft.com/xxxxxxx/Tables/timesheet"
    
    try:
        # Initialize Spark
        spark = initialize_spark()
        print("Spark session initialized")
        
        # Load latest CSV into Spark DataFrame
        df = load_latest_csv_to_spark(spark, lakehouse_path)
        
        if df is not None:
                      
            # Write to Delta table
            write_to_delta_table(df, delta_table_path, mode="append")
            
            # Show data from Delta table
            delta_df = spark.read.format("delta").load(delta_table_path)
        
            return df
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        return None
    finally:
        print("Process completed")

if __name__ == "__main__":
    df = main()