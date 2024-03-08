from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import time

def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("date","category")
    df = df.withColumn("total_price_per_category_per_day",
                       f.sum("price").over(window_spec))
    return df

def calculate_total_price_per_category_per_day_last_30_days(df):
    window_spec = Window.partitionBy("date","category").orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum(
        "price").over(window_spec))
    return df.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days") 

def main():
    spark = SparkSession.builder \
        .appName("no_udf") \
        .master("local[*]") \
        .getOrCreate()

    df_base = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)
    start_no_udf = time.time()
    df = df_base.withColumn("category_name", f.when(f.col("category") < 6, "food").otherwise("furniture"))
    df.write.csv("res.csv", header=True, mode="overwrite")
    end_no_udf = time.time()
    time_no_udf = end_no_udf - start_no_udf

    print("no_udf", time_no_udf)

    df_total_price_per_day = calculate_total_price_per_category_per_day(df)
    df_last_30_days = calculate_total_price_per_category_per_day_last_30_days(df_total_price_per_day)    

    spark.stop()

if __name__ == "__main__":
    main()