#!/usr/bin/env python
# coding: utf-8

# # Average problem
# 

# ### Configurations

# In[1]:


import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',

    datefmt='%Y-%m-%d,%H:%M:%S:%f', level=logging.INFO)


# In[2]:


logging.info('Application Start!!')


# ### Get problems

# In[5]:


import findspark
findspark.init()


# In[6]:


from pyspark import SparkConf, SparkContext, SQLContext


conf = SparkConf().setMaster("yarn").setAppName("bigdata-group1-avg-problems")
sc = SparkContext.getOrCreate(conf = conf)
sqlContext = SQLContext(sc)


# In[39]:


problem_df = sqlContext.read.option("multiline", "true").json("data/newformat_problem.json")


# In[10]:


problem_df.printSchema()


# In[40]:


from pyspark.sql import functions as F
explode_df = problem_df.withColumn("type", F.explode("type"))
typed_df = explode_df.filter(F.col("type") != "UNRANK")


# In[17]:


typed_df.count()


# In[18]:


typed_df.na.drop("all").count()


# In[44]:


score_map = {"브론즈 5": 1, "브론즈 4": 2, "브론즈 3": 3, "브론즈 2": 4, "브론즈 1": 5,
            "실버 5": 6, "실버 4": 7, "실버 3": 8, "실버 2": 9, "실버 1": 10,
            "골드 5": 11, "골드 4": 12, "골드 3": 13, "골드 2": 14, "골드 1": 15,
            "플래티넘 5": 16, "플래티넘 4": 17, "플래티넘 3": 18, "플래티넘 2": 19, "플래티넘 1": 20,
             "다이아몬드 5": 21, "다이아몬드 4": 22, "다이아몬드 3": 23, "다이아몬드 2": 24, "다이아몬드 1": 25,
             "루비 5": 26, "루비 4": 27, "루비 3": 28, "루비 2": 29, "루비 1": 30,
             "UNRANK": 5
            }


# In[45]:


score_udf = F.udf(lambda x: score_map[x])
score_df = typed_df.withColumn("difficulty_score", score_udf(F.col("difficulty")))


# In[46]:


score_df.filter(F.col("type") == "UNRANK").show()


# In[49]:


avg_score_df = score_df.groupBy("type").agg(F.count("*").alias("count"), F.avg("difficulty_score").alias("avg_of_difficulty"))
avg_score_df.show()


# In[51]:


avg_score_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/problem/problem_avg_difficulty")


# In[52]:


sc.stop()

