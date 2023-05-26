from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("main").getOrCreate()

df_accounts_data = spark.read.option("header", "true").option("inferSchema","true")\
    .csv("Spark_Exam/data/account_data.txt")

df_customer_data = spark.read.option("header", "true").option("inferSchema","true")\
    .csv("Spark_Exam/data/customer_data.txt")


df_customer_data.show()

df_accounts_data.show()
