import sys
import json
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Task1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext


data_path = sys.argv[1]

articles_rdd = sc.wholeTextFiles(data_path + "/*.json")

articles_data_rdd = articles_rdd.map(lambda record: json.loads(record[1])).map(
    lambda article: (str(article["article_id"]), article["article_body"])
)



print(article_data_rdd.take(5))