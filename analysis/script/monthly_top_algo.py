import logging
import findspark
from pyspark import SparkConf, SparkContext, SQLContext
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',

datefmt='%Y-%m-%d,%H:%M:%S:%f', level=logging.INFO)

logging.info('Application Start!!')
findspark.init()

# ### Get trials
conf = SparkConf().setMaster("yarn").setAppName("bigdata-group1-montly")
sc = SparkContext.getOrCreate(conf = conf)
sqlContext = SQLContext(sc)

trial_df = sqlContext.read.format("csv").option("encoding", "UTF-8").option("header","true").load("data/trials.csv")
trial_df = trial_df.na.drop(subset=["type"])


# #### Feature engineering
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
trial_split_df = trial_df.withColumn("type", F.split(F.col("type"), '/'))

def cut_tail(array):
    return array[:-1]
cut_tail_udf = F.udf(cut_tail, ArrayType(StringType()))
trial_typelist_df = trial_split_df.withColumn("type", cut_tail_udf("type"))

trial_per_type_df = trial_typelist_df.withColumn("month", F.split(F.col("date"), "-")[1])     .select(F.explode("type").alias("Type"), "user", "questionId", "month")     .groupBy("month", "Type").count()
trial_per_type_df.persist()

for month in range(1, 6):
    month_top_df = trial_per_type_df.filter(F.col("month").like("%{}".format(str(month))))     .sort(F.col("count").desc())
    month_top_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/monthly_count/{}".format(str(month)))

sc.stop()

