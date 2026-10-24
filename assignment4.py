import sys
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def _fmt(v):
    """Convert one cell value to the text that is printed."""
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)

def print_df(task_name, df, sort_cols):
    """Print a deterministically sorted DataFrame for autograding."""
    ordered = df.orderBy(*sort_cols)
    print(f"BEGIN_{task_name}")
    print("\t".join(ordered.columns))
    for row in ordered.toLocalIterator():
        print("\t".join(_fmt(v) for v in row))
    print(f"END_{task_name}")
    print()  # empty line between blocks, for readability


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

place_article_rdd = article_data_rdd.flatMap(extract_pairs).distinct()


sorted_rdd = place_article_rdd.sortBy(lambda x: (x[0], x[1]))

total_rows = sorted_rdd.count()

print("BEGIN_TASK1_RDD")
print(total_rows)


for place, article_id in sorted_rdd.take(20):
    print(f"{place}\t{article_id}")

print("END_TASK1_RDD")
print()



#task2

pairs_df = spark.createDataFrame(place_article_rdd, ["place", "article_id"])

print_df('TASK2_PAIRS', pairs_df.orderBy('place', 'article_id').limit(20),
         [F.col('place').asc(), F.col('article_id').asc()])



#task3

words_df = article_data_rdd.flatMap(
    lambda x: [(x[0], word) 
               for word in re.findall(r'[a-z]+', x[1].lower())
               if word in places]).toDF(["article_id", "place"])

frequencies_df = words_df.groupBy("article_id", "place").agg(F.count("*").alias("term_frequency")).select("article_id", "place", "term_frequency")


article_window = Window.partitionBy("article_id")

dominant_df = frequencies_df.withColumn("max_frequency", F.max("term_frequency").over(article_window)).filter(F.col("term_frequency") == F.col("max_frequency")).select("article_id", "place", "term_frequency")

print_df("TASK3_FREQUENCIES",frequencies_df,[F.col("article_id").asc(), F.col("place").asc()])
print_df("TASK3_DOMINANT",dominant_df,[F.col("article_id").asc(), F.col("place").asc()])


#task4 Blind spots

place_counts_df = pairs_df.groupBy("place").agg(
    F.countDistinct("article_id").alias("article_count")
)

blind_spots_df = pairs_df.join(place_counts_df.filter(F.col("article_count") == 1).select("place"),
    on="place",
    how="inner"
).select("place", "article_id")

print_df(
    "TASK4_BLIND_SPOTS",
    blind_spots_df,
    [F.col("place").asc(), F.col("article_id").asc()]
)



#task5

place_articles = pairs_df.select("place", "article_id").distinct()

# print(place_articles.head(20),end="\n")

place_totals = place_articles.groupBy("place").agg(F.countDistinct("article_id").alias("count_a"))

pairs = place_articles.alias("a").join(
    place_articles.alias("b"),
    (F.col("a.article_id") == F.col("b.article_id")) &
    (F.col("a.place") < F.col("b.place")),
    "inner"
)

cooccurrence = pairs.groupBy(F.col("a.place").alias("p1"), F.col("b.place").alias("p2")).agg(F.countDistinct("a.article_id").alias("cooccurrence_count"))


jaccard_df = cooccurrence.join(
    place_totals.withColumnRenamed("place", "p1"),
    "p1"
).withColumnRenamed("count_a", "count_p1").join(
    place_totals.withColumnRenamed("place", "p2"),
    "p2"
).withColumnRenamed("count_a", "count_p2")

jaccard_df = jaccard_df.withColumn(
    "jaccard",
    F.col("cooccurrence_count") /
    (F.col("count_p1") + F.col("count_p2") -
     F.col("cooccurrence_count"))
).select("p1", "p2", "jaccard", "cooccurrence_count")


top5 = jaccard_df.orderBy(
    F.col("jaccard").desc(),
    F.col("p1").asc(),
    F.col("p2").asc()
).limit(5)

print_df(
    "TASK5_TOP5",
    top5,
    [F.col("jaccard").desc(), F.col("p1").asc(), F.col("p2").asc()]
)

bottom5 = jaccard_df.orderBy(
    F.col("jaccard").asc(),
    F.col("p1").asc(),
    F.col("p2").asc()
).limit(5)

print_df(
    "TASK5_BOTTOM5",
    bottom5,
    [F.col("jaccard").asc(), F.col("p1").asc(), F.col("p2").asc()]
)


place_frequency_df = words_df.groupBy("place").agg(
    F.count("*").alias("total_mentions"),
    F.countDistinct("article_id").alias("article_count")
)

top5_places = place_frequency_df.orderBy(
    F.col("total_mentions").desc(),
    F.col("place").asc()
).limit(5)

print_df(
    "TASK6_TOP5",
    top5_places.select("place", "total_mentions", "article_count"),
    [F.col("total_mentions").desc(), F.col("place").asc()]
)
#task7
article_place_counts = pairs_df.groupBy("article_id").agg(
    F.countDistinct("place").alias("distinct_place_count")
)

top10_articles = article_place_counts.orderBy(
    F.col("distinct_place_count").desc(),
    F.col("article_id").asc()
).limit(10)

print_df(
    "TASK7_TOP10",
    top10_articles.select("article_id", "distinct_place_count"),
    [F.col("distinct_place_count").desc(), F.col("article_id").asc()]
)