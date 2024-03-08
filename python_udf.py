import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_category(cat):
    cat = int(cat) 
    if cat < 6:
        return "food"
    else:
        return "furniture"

udf_categorize_category = udf(categorize_category, StringType())

def main():

    spark = SparkSession.builder \
        .appName("python_udf")\
        .master("local[*]")\
        .getOrCreate()
    
    print("start")
    start = time.time()
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    df = df.withColumn("category_name", udf_categorize_category(df["category"]))
    df.write.csv("res.csv", header=True, mode="overwrite")
    end = time.time()
    time_final = end - start
    print("python_UDF : ", time_final)

    spark.stop()

if __name__ == "__main__":
    main()