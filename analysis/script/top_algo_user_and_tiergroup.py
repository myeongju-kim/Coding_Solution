import logging
import findspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',

    datefmt='%Y-%m-%d,%H:%M:%S:%f', level=logging.INFO)
logging.info('Application Start!!')

# ### Get trials

findspark.init()
conf = SparkConf().setMaster("yarn").setAppName("bigdata-group1-top-algo")
sc = SparkContext.getOrCreate(conf = conf)
sqlContext = SQLContext(sc)
trial_df = sqlContext.read.format("csv").option("encoding", "UTF-8").option("header","true").load("data/trials.csv")

# Drop missing type
trial_df = trial_df.na.drop(subset=["type"])
trial_split_df = trial_df.withColumn("type", F.split(F.col("type"), '/'))

def cut_tail(array):
    return array[:-1]
cut_tail_udf = F.udf(cut_tail, ArrayType(StringType()))
trial_typelist_df = trial_split_df.withColumn("type", cut_tail_udf("type"))

# Categorize submit results
correct_keyword = ["맞았습니다"]
wrong_keyword = ["초과", "틀렸습니다"]
error_keyword = ["런타임", "컴파일", "출력"]
def categorize(x):
    for keyword in correct_keyword:
        if keyword in x:
            return "맞았습니다"
    for keyword in wrong_keyword:
        if keyword in x:
            return "틀렸습니다"
    for keyword in error_keyword:
        if keyword in x:
            return "에러"
    return "기타"

categorize_udf = F.udf(categorize, StringType())
categorized_df = trial_typelist_df.withColumn("result", categorize_udf(F.col("result")))

# Leave only correct and wrong result.
cor_wro_df = categorized_df.withColumn("type", F.explode("type")).filter((F.col("result") == "맞았습니다") | (F.col("result") == "틀렸습니다"))

user_trial_df =  cor_wro_df.groupBy('user','user_rank', "type", "result").count()
user_trial_df.persist()
user_stat_df = user_trial_df.groupBy('user','user_rank' , 'type') .agg(F.sum("count").alias("submit"),      F.sum(F.when(F.col("result") == "맞았습니다", F.col("count"))).alias("correct"),      F.sum(F.when(F.col("result") == "틀렸습니다", F.col("count"))).alias("wrong")     ).na.fill(0)    .withColumn("rate", F.col("correct") / F.col("submit"))    .sort("user", F.col("rate").desc())
user_stat_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/stat_correct/algo_correct_stat_user")


group_trial_df =  cor_wro_df.groupBy('user_rank', "type", "result").count()
group_trial_df.persist()
group_stat_df = group_trial_df.groupBy('user_rank' , 'type') .agg(F.sum("count").alias("submit"),      F.sum(F.when(F.col("result") == "맞았습니다", F.col("count"))).alias("correct"),      F.sum(F.when(F.col("result") == "틀렸습니다", F.col("count"))).alias("wrong")     ).na.fill(0)    .withColumn("rate", F.col("correct") / F.col("submit"))    .sort("user_rank", F.col("rate").desc())
group_stat_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/stat_correct/algo_correct_stat_rankgroup")

sc.stop()

