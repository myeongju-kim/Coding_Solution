# Simple deployment diagram
![deployment](https://user-images.githubusercontent.com/49145596/174280697-c6d8d4d0-9e65-45e3-a22c-785009c9bfa2.png)

# Hadoop config
In `hadoop`, there are config files for Hadoop & HDFS & Yarn for master and slaves. <br>
In addition, it is necessary to set environmental variables such as hadoop-env.sh settings, HADOOP_HOME, JAVA_HOME, and CLASS_PATH.

# Spark config
In `spark`, there is config file `spark-env.sh` <b>without master ip</b><br>
In addition, it is necessary to set environmental variables such as PYSPARK_PYTHON, SPARK_CONF_DIR, etc
