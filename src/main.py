# Create a Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType, DoubleType


# Make an object
spark = SparkSession.builder.appName("main").getOrCreate()

df_accounts_data = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Spark_Exam/data/account_data.txt")  # Creating a DataFrame using a CSV file

df_customer_data = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Spark_Exam/data/customer_data.txt")  # Creating a DataFrame using a CSV file



# Perform a join operation
CustomerAccountOutput = df_accounts_data.join(df_customer_data, "customerId")


# Add additional columns
CustomerAccountOutput = CustomerAccountOutput.groupBy("customerId", "forename", "surname").agg(
    count("accountId").alias("numberAccounts"),
    sum("balance").alias("totalBalance"),
    avg("balance").alias("averageBalance")
)




CustomerAccountOutput_schema = StructType([
    StructField("customerId", StringType(), nullable=True),
    StructField("forename", StringType(), nullable=True),
    StructField("surname", StringType(), nullable=True),
    StructField("accounts", ArrayType(ArrayType(StringType())), nullable=True),
    StructField("numberAccounts", LongType(), nullable=True),
    StructField("totalBalance", LongType(), nullable=True),
    StructField("averageBalance", DoubleType(), nullable=True)
])
# Show the joined DataFrame with additional columns
#CustomerAccountOutput.show()

# Save the joined DataFrame with additional columns as a Parquet file
#CustomerAccountOutput.write.parquet("output.parquet")


parquet_file_path = "Spark_Exam/output.parquet/part-00000-ba1c5421-e7c6-4f19-a0de-63c01ffdafdd-c000.snappy.parquet"
parquet_df = spark.read.parquet(parquet_file_path)

# Display the data
parquet_df.show()
