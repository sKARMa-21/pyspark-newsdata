import sys
import json
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Task1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext


data_path = sys.argv[1]

articles_rdd = sc.wholeTextFiles(data_path + "/*.json")

article_data_rdd = articles_rdd.map(lambda record: json.loads(record[1])).map(
    lambda article: (str(article["article_id"]), article["article_body"])
)

places = [
    "delhi", "mumbai", "kolkata", "chennai", "bengaluru",
    "hyderabad", "lucknow", "srinagar", "kashmir", "punjab",
    "gujarat", "maharashtra", "kerala", "karnataka", "bengal",
    "assam", "bihar", "rajasthan", "pakistan", "china",
    "noida", "mathura"
]

def extract_pairs(article):
    article_id, body = article
    words = set(re.findall(r'[a-z]+', body.lower()))
    return[(place, article_id) for place in places if place in words]

palce_article_rdd = article_data_rdd.flatMap(extract_pairs).distinct()


sorted_rdd = palce_article_rdd.sortBy(lambda x: (x[0], x[1]))

total_rows = sorted_rdd.count()

print("BEGIN_TASK1_RDD")
print(total_rows)


for place, article_id in sorted_rdd.take(20):
    print(f"{place}\t{article_id}")

print("END_TASK1_RDD")
print()
# print(article_data_rdd.take(5))