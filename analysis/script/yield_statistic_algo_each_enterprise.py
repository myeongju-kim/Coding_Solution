#!/usr/bin/env python
# coding: utf-8

# # Yield statistics for algorithms each enterprise
# 

# ### Configurations

# In[43]:


import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',

    datefmt='%Y-%m-%d,%H:%M:%S:%f', level=logging.INFO)


# In[44]:


logging.info('Application Start!!')


# ### Get company

# In[45]:


import findspark
findspark.init()


# In[46]:


from pyspark import SparkConf, SparkContext, SQLContext


conf = SparkConf().setMaster("yarn").setAppName("bigdata-group1-top-algo")
sc = SparkContext.getOrCreate(conf = conf)
sqlContext = SQLContext(sc)


# In[47]:


company_df = sqlContext.read.option("multiline", "true").json("data/companys.json")


# In[48]:


company_df.printSchema()


# In[20]:


company_df.groupBy('difficulty').count().show()


# In[49]:


from pyspark.sql import functions as F
score_map = {"브론즈 5": 1, "브론즈 4": 2, "브론즈 3": 3, "브론즈 2": 4, "브론즈 1": 5,
            "실버 5": 6, "실버 4": 7, "실버 3": 8, "실버 2": 9, "실버 1": 10,
            "골드 5": 11, "골드 4": 12, "골드 3": 13, "골드 2": 14, "골드 1": 15,
            "플래티넘 5": 16, "플래티넘 4": 17, "플래티넘 3": 18, "플래티넘 2": 19, "플래티넘 1": 20,
             "다이아몬드 5": 21, "다이아몬드 4": 22, "다이아몬드 3": 23, "다이아몬드 2": 24, "다이아몬드 1": 25,
             "루비 5": 22, "루비 4": 22, "루비 3": 23, "루비 2": 24, "루비 1": 25,
            }


# In[50]:


score_udf = F.udf(lambda x: score_map[x])
score_df = company_df.withColumn("difficulty_score", score_udf(F.col("difficulty")))


# In[51]:


type_df = score_df.withColumn("type", F.explode("type"))


# In[32]:


score_df.na.drop(subset=["type"]).withColumn("type", F.explode("type")).count()


# In[52]:


company_type_g_df = type_df.groupBy("company", "type")   .agg(F.count('type').alias("count"), F.avg("difficulty_score").alias("avg_of_difficulty"))   .sort("company", F.col("count").desc())
company_type_g_df.persist()


# In[53]:


company_type_g_df.show()


# In[41]:


company_type_g_df.write.format("csv").mode('overwrite')     .option("encoding", "UTF-8").save("static/stat_enterprise/stat_each_enterprise")


# In[1]:


sc.stop()

