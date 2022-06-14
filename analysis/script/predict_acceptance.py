#!/usr/bin/env python
# coding: utf-8
import logging
import findspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import itertools
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import norm
import scipy.special
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix, DenseMatrix
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.ml.feature import StandardScaler
from scipy.stats import norm
import scipy.special
# # Predict acceptance to each enterprise
# 
# #### Key feature
# - Correlation analysis between algo types & feature selection
# - Dimensionality Reduction using Truncated Singlular Value Decomposition (SVD) (to 2D)
# - Normalize feature to Bivariate Normal Distribution & Visualize
# - Build Linear Model for acceptance prediction
# - Calculate the Probability that each user will be accepted to each company & Save



logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',

    datefmt='%Y-%m-%d,%H:%M:%S:%f', level=logging.INFO)
logging.info('Application Start!!')


findspark.init()

conf = SparkConf().setMaster("yarn").setAppName("bigdata-group1-predict-acceptance")
sc = SparkContext.getOrCreate(conf = conf)
sqlContext = SQLContext(sc)


company_schema = StructType([
    StructField("company", StringType()),
    StructField("type", StringType()),
    StructField("count", IntegerType()),
    StructField("avg_of_difficulty", FloatType())
])
company_df = sqlContext.read.format("csv").option("encoding", "UTF-8").schema(company_schema).load("static/stat_enterprise/stat_each_enterprise")

user_schema = StructType([
    StructField("user", StringType()),
    StructField("user_rank", StringType()),
    StructField("type", StringType()),
    StructField("total_submit", IntegerType()),
    StructField("correct", IntegerType()),
    StructField("wrong", IntegerType()),
    StructField("correct_rate", FloatType())
])
user_df = sqlContext.read.format("csv").option("encoding", "UTF-8").schema(user_schema).load("static/stat_correct/algo_correct_stat_user")


problem_schema = StructType([
    StructField("type", StringType()),
    StructField("count", IntegerType()),
    StructField("avg_of_difficulty", FloatType())
])

problem_df = sqlContext.read.format("csv").option("encoding", "UTF-8").schema(problem_schema).load("static/problem/problem_avg_difficulty")

company_df.groupBy("company").count().show()

company_name_rows = company_df.groupBy("company").count().select("company").collect()
companys = list(map(lambda x: x[0], company_name_rows))

company_stat = []

company_df.filter(F.col("company") == "line").sort(F.col("count").desc()).head(10)
company_df.filter(F.col("company") == "kakao").sort(F.col("count").desc()).head(10)
company_df.filter(F.col("company") == "naver").sort(F.col("count").desc()).head(10)
company_df.filter(F.col("company") == "samsung").sort(F.col("count").desc()).head(10)
company_df.filter(F.col("company") == "coupang").sort(F.col("count").desc()).head(10)


for company in companys:
    top5_rows = company_df.filter(F.col("company") == company).sort(F.col("count").desc()).head(5)
    company_stat.append({"company": company, "top1": top5_rows[0][1], "top2": top5_rows[1][1], "top3": top5_rows[2][1], "top4": top5_rows[3][1], "top5": top5_rows[4][1]})

company_stat_df = sqlContext.createDataFrame(company_stat)
company_stat_df.show()

# 브루트포스 알고리즘 == 브루트포스
for i in range(1, 6):
    col = "top{}".format(str(i))
    company_stat_df = company_stat_df.withColumn(col, F.when(F.col(col) == "브루트포스", "브루트포스 알고리즘").otherwise(F.col(col)))
company_stat_df.show()

test_top10_bank = {"line": [], "coupang": [], "naver": [], "kakao": [], "samsung": []}
for company in test_top10_bank.keys():
    for row in company_df.filter(F.col("company") == company).sort(F.col("count").desc()).head(10):
        test_top10_bank[company].append(row[1])

test_bank = set(list(itertools.chain(*test_top10_bank.values())))
test_bank

algo_df = user_df.filter(F.col("type").isin(test_bank)).groupBy("type").agg(F.sum("total_submit").alias("total_submit") ,F.sum("correct").alias("correct") ,F.sum("wrong").alias("wrong"))
algo_df.filter((F.col("type") == "브루트포스 알고리즘") |  (F.col("type") == "해시를 사용한 집합과 맵") | (F.col("type") == "자료구조")).show()

test_bank_deduplicate_df = test_bank - set(["브루트포스 알고리즘", "해시를 사용한 집합과 맵", "자료구조"])

test_bank_df = algo_df.filter(F.col("type").isin(test_bank_deduplicate_df))    .withColumn("total_submit", F.when(F.col("type") == "브루트포스", F.col("total_submit") + brute[1])                .when(F.col("type") == "해시", F.col("total_submit") + hashed[1])                .otherwise(F.col("total_submit")))    .withColumn("correct", F.when(F.col("type") == "브루트포스", F.col("correct") + brute[2])                .when(F.col("type") == "해시", F.col("correct") + hashed[2])                .otherwise(F.col("correct")))     .withColumn("wrong", F.when(F.col("type") == "브루트포스", F.col("wrong") + brute[3])                .when(F.col("type") == "해시", F.col("wrong") + hashed[3])                .otherwise(F.col("wrong")))     .withColumn("correct_rate", F.col("correct") / F.col("total_submit"))
joined_bank_df = test_bank_df.join(problem_df.select("type", "avg_of_difficulty"), ["type"], "left_outer")


# # Dimensonality Reduction
# ## Correlation

transposed_bank_df = sqlContext.createDataFrame(joined_bank_df.toPandas().set_index("type").transpose())
transposed_bank_pd = joined_bank_df.toPandas().set_index("type").transpose()
transposed_bank_pd.corr()



plt.figure(figsize=(15,15))
sns.heatmap(data = transposed_bank_pd.corr(), annot=True, 
fmt = '.2f', linewidths=.5, cmap='Blues')

user_grouped_df = user_df.groupBy("user").agg(F.collect_list("type"), F.collect_list("total_submit"), F.collect_list("correct"), F.collect_list("wrong"), F.collect_list("correct_rate"))






square_root_two = 2 ** (1/2)
inferior_cdf_udf = F.udf(lambda x, y: norm.cdf((x+y) / square_root_two).tolist(), FloatType())


selection_ratio = {"line": 0.25, "kakao": 0.15, "samsung": 0.15, "naver": 0.15, "coupang": 0.20}





def sum_binomial_pdf(p, inverse_k_min, n=100):
    k_min = int(n* (1-inverse_k_min))
    probability = 0.0
    for k in range(k_min, n+1):
        probability += scipy.special.comb(n, k).tolist() * (p ** k) * ((1-p) ** (n-k))
    return probability

sum_binomial_pdf_udf = F.udf(sum_binomial_pdf, FloatType())
inferior_cdf_df.withColumn("acceptance_probability_line", sum_binomial_pdf_udf(F.col("inferior"), F.lit(selection_ratio["line"]))).show()



def top5_rate(type_list, rate_list, company):
    _enter_top5 = {"line": ["구현", "브루트포스 알고리즘", "문자열", "자료 구조", "수학"],
             "kakao": ["구현", "문자열", "브루트포스 알고리즘", "그래프 이론", "너비 우선 탐색"],
             "naver": ["구현", "자료 구조", "문자열", "브루트포스 알고리즘", "수학"],
             "samsung": ["구현", "시뮬레이션", "브루트포스 알고리즘", "그래프 이론", "너비 우선 탐색"],
             "coupang": ["문자열", "구현", "자료 구조", "수학", "그래프 탐색"]}
    top5 = _enter_top5[company]
    ret = []
    for _type in top5:
        if _type in type_list:
            ret.append(rate_list[type_list.index(_type)])
        else:
            ret.append(0.0)
    return ret

top5_rate_udf = F.udf(top5_rate, ArrayType(FloatType()))



enter_top5 = {"line": ["구현", "브루트포스 알고리즘", "문자열", "자료 구조", "수학"],
             "kakao": ["구현", "문자열", "브루트포스 알고리즘", "그래프 이론", "너비 우선 탐색"],
             "naver": ["구현", "자료 구조", "문자열", "브루트포스 알고리즘", "수학"],
             "samsung": ["구현", "시뮬레이션", "브루트포스 알고리즘", "그래프 이론", "너비 우선 탐색"],
             "coupang": ["문자열", "구현", "자료 구조", "수학", "그래프 탐색"]}




def sum_binomial_pdf(p, inverse_k_min, n=100):
    k_min = int(n* (1-inverse_k_min))
    probability = 0.0
    for k in range(k_min, n+1):
        probability += scipy.special.comb(n, k).tolist() * (p ** k) * ((1-p) ** (n-k))
    return probability


# In[343]:


enter_top5 = {"line": ["구현", "브루트포스 알고리즘", "문자열", "자료 구조", "수학"],
             "kakao": ["구현", "문자열", "브루트포스 알고리즘", "그래프 이론", "너비 우선 탐색"],
             "naver": ["구현", "자료 구조", "문자열", "브루트포스 알고리즘", "수학"],
             "samsung": ["구현", "시뮬레이션", "브루트포스 알고리즘", "그래프 이론", "너비 우선 탐색"],
             "coupang": ["문자열", "구현", "자료 구조", "수학", "그래프 탐색"]}


# In[401]:



user_result_df = user_grouped_df.select("user")
company_reflect_stat = []

for idx, cur_company in enumerate(companys):
    company_merged_df = user_grouped_df.select("user", top5_rate_udf(F.col("collect_list(type)"), F.col("collect_list(correct_rate)"), F.lit(cur_company)).alias("merged"))
    user_company_df = company_merged_df
    for idx2, algo_type in enumerate(enter_top5[cur_company]):
        user_company_df = user_company_df.withColumn(algo_type, F.col("merged")[idx2])
    user_company_df = user_company_df.drop("merged")
    id_company_df = user_company_df.withColumn("id", F.monotonically_increasing_id())
    vecAssembler = VectorAssembler(inputCols=user_company_df.drop("user").columns, outputCol="features")
    vector_df = vecAssembler.transform(user_company_df.drop("user")).select("features")
    temp_rdd = vector_df.rdd.map(lambda x: Vectors.fromML(x[0]))
    mat = RowMatrix(temp_rdd)
    svd = mat.computeSVD(2, computeU=True)
    U = svd.U       # Left Singular Vector
    s = svd.s       # 2 Singular value
    V = svd.V       # Right Singular Vector
    v = V.toArray()
    v_t = np.transpose(v)
    
    company_stat_dict = {"company": cur_company}
    for i in range(1, 6):
        col = "top{}_reflected".format(str(i))
        company_stat_dict[col] = "score1" if v_t[0][i-1] > v_t[1][i-1] else "score2"
    company_reflect_stat.append(company_stat_dict)
    truncated_score_df = U.rows.map(lambda x: {"score1": float(x[0]), "score2": float(x[1])}).toDF().withColumn("id", F.monotonically_increasing_id())
    company_scored_df = id_company_df.join(truncated_score_df, "id")
    company_scored_df.persist()

    assembler_1 = VectorAssembler(inputCols=["score1"],outputCol="vectored_score1")
    vectored_df_1 = assembler_1.transform(company_scored_df)
    assembler_2 = VectorAssembler(inputCols=["score2"],outputCol="vectored_score2")
    vectored_df = assembler_2.transform(vectored_df_1)
    vectored_df.select("vectored_score1", "vectored_score2").show(truncate=False)

    scaler_1 = StandardScaler(inputCol="vectored_score1", outputCol="scaled_score1",
                        withStd=True, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scale_model_1 = scaler_1.fit(vectored_df)

    # Normalize each feature to have unit standard deviation.
    scaled_1 = scale_model_1.transform(vectored_df)


    scaler_2 = StandardScaler(inputCol="vectored_score2", outputCol="scaled_score2",
                        withStd=True, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scale_model_2 = scaler_2.fit(scaled_1)

    # Normalize each feature to have unit standard deviation.
    scaled_df = scale_model_2.transform(scaled_1)
    extract_udf = F.udf(lambda x: x.tolist()[0], FloatType())
    stat_df = scaled_df.withColumn("scaled_score1", extract_udf(F.col("scaled_score1")))         .withColumn("scaled_score2", extract_udf(F.col("scaled_score2")))         .select("user", "scaled_score1", "scaled_score2")

    
    square_root_two = 2 ** (1/2)
    inferior_cdf_udf = F.udf(lambda x, y: norm.cdf((x+y) / square_root_two).tolist(), FloatType())
    inferior_cdf_df =stat_df.withColumn("inferior", inferior_cdf_udf(F.col("scaled_score1"), F.col("scaled_score2")))         .withColumn("superior", F.lit(1) - F.col("inferior"))
    
    sum_binomial_pdf_udf = F.udf(sum_binomial_pdf, FloatType())
    probability_df = inferior_cdf_df.withColumn("acceptance_probability", sum_binomial_pdf_udf(F.col("inferior"), F.lit(selection_ratio[cur_company])))
    probability_df = probability_df.withColumnRenamed("scaled_score1", "{}_score1".format(cur_company))     .withColumnRenamed("scaled_score2", "{}_score2".format(cur_company))     .withColumnRenamed("inferior", "{}_inferior".format(cur_company))     .withColumnRenamed("superior", "{}_superior".format(cur_company))     .withColumnRenamed("acceptance_probability", "{}_acceptance_probability".format(cur_company))
    user_result_df = user_result_df.join(probability_df, "user")



user_result_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/predict_acceptance/user_with_probabilities")



company_reflect_df = sqlContext.createDataFrame(company_reflect_stat)
company_stat_df = company_stat_df.join(company_reflect_df, "company")



company_stat_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/predict_acceptance/company_top5_score_criteria")


sc.stop()

