#Create a spark session 
from pyspark.sql import SparkSession

#Makes an object
spark = SparkSession.builder.appName("main").getOrCreate()

df_accounts_data = spark.read.option("header", "true").option("inferSchema","true")\
    .csv("Spark_Exam/data/account_data.txt") # Creating a data frame using a csv file 

df_customer_data = spark.read.option("header", "true").option("inferSchema","true")\
    .csv("Spark_Exam/data/customer_data.txt") # Creating a data frame using a csv file 


# Perform a join operation
joined_df = df_accounts_data.join(df_customer_data, "customerId")

# Show the joined DataFrame
joined_df.show()
