# Create a Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, collect_list
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType, DoubleType
from dataclasses import dataclass


# Create a Spark session
spark = SparkSession.builder.appName("spark").getOrCreate()

# Extract address components such as street number, street name, city, country
def parse_address(address):

    # Split the address by commas
    address_parts = address.split(',')

    # accessing the elements using indexing 
    # strip() - removes any empty spaces around the data
    if len(address_parts) == 4:
        streetNumber = address_parts[0].strip()
        streetName = address_parts[1].strip()
        city = address_parts[2].strip()
        country = address_parts[3].strip()

    # creating a dictionary with parsed address components
        parsed_address = {
            'streetNumber': streetNumber,
            'streetName': streetName,
            'city': city,
            'country': country
        }
    else:
        # If the address format is not as expected, possibly missing a comma the else statement will print blank entries 
        parsed_address = {
            'streetNumber': '',
            'streetName': '',
            'city': '',
            'country': ''
        }
    # Return the parsed address as a dictionary containing the address fields
    return parsed_address




# Define the CustomerAccount dataclass
@dataclass
class CustomerAccount:
    customerId: str
    forename: str
    surname: str
    accounts: list
    streetNumber: int
    streetName: str
    city: str
    country: str

# Creating a list to store the customer account objects
customerAccounts = []


# Defining the structure and data types of a DataFrame, it specifies the column names, their data types. 
# Null=True: Fields can be left empty or have missing values  
# The schrma will help making the code more type safe
schema = StructType([
    StructField("customerId", StringType(), nullable=True),
    StructField("forename", StringType(), nullable=True),
    StructField("surname", StringType(), nullable=True),
    StructField("accounts", ArrayType(StringType()), nullable=True),
    StructField("numberAccounts", LongType(), nullable=True),
    StructField("totalBalance", LongType(), nullable=True),
    StructField("averageBalance", DoubleType(), nullable=True),
    StructField("address", StructType([
        StructField("streetNumber", LongType(), nullable=True),
        StructField("streetName", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True)
    ]))
])


# Read account data from CSV - ACCOUNTS DATA
df_accounts_data = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv("Spark_Exam/data/account_data.txt") 

# Read customer data from CSV - CUSTOMER DATA
df_customer_data = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv("Spark_Exam/data/customer_data.txt") 

# Performing a join operation.  (accounts_data / customer data)
Customer_Account_Output_Data = df_accounts_data.join(df_customer_data, "customerId")

# Add additional columns
# GroupBy: "customerId", "forename", "surname" as we need these columns for reference
Customer_Account_Output_Data_addedFields = Customer_Account_Output_Data.groupBy("customerId", "forename", "surname").agg(
    collect_list("accountId").alias("accounts"),
    count("accountId").alias("numberAccounts"),
    sum("balance").alias("totalBalance"),
    avg("balance").alias("averageBalance")
)
# print tables - QUESTION 1 TABLE
Customer_Account_Output_Data_addedFields.show()

# Save DataFrame as Parquet file
# Customer_Account_Output_Data_addedFields.write.parquet("Spark_Exam/data")



# Read the Parquet file from 
parquet_df = spark.read.parquet("Spark_Exam/data/output.parquet/part-00000-164e172d-36bd-4763-a1e2-f73c00e04c02-c000.snappy.parquet")

# Read address data from CSV - ADDRESS DATA
df_address_data = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv("Spark_Exam/data/address_data.txt") 

# Performing a join between parquet_df and df_address_data in the customerId column
address_joined_parquet_df = parquet_df.join(df_address_data, "customerId")

# Prints to joined data
address_joined_parquet_df.show()


# Iterating over each row of the DataFrame 
for row in address_joined_parquet_df.collect():
    
    # extracting the values of specific columns
    customerId = row.customerId
    forename = row.forename
    surname = row.surname
    accounts = row.accounts
    address = row.address  


    # Calling the "parse_address" function to extract data from the column 'address'
    parsed_address = parse_address(address)

    # Creating CustomerAccount objects by iterating over the rows in the DataFrame, extracting the column values
    customer_account = CustomerAccount(
        customerId=customerId,
        forename=forename,
        surname=surname,
        accounts=accounts,
        streetNumber=parsed_address['streetNumber'],
        streetName=parsed_address['streetName'],
        city=parsed_address['city'],
        country=parsed_address['country']
    )

    # Appending/adding the CustomerAccount objects to the CustomeerAccouts list
    customerAccounts.append(customer_account)



# Creates a Spark DataFrame from the list "customerAccounts"
Customer_Account_Objects = spark.createDataFrame(customerAccounts)

# Defines the column order
column_order = [
    "customerId",
    "forename",
    "surname",
    "accounts",
    "streetNumber",
    "streetName",
    "city",
    "country"
    ]
                                
# Select columns in the desired order through "column_order"
Customer_Account_Parsed_Address = Customer_Account_Objects.select(*column_order)
                                

# Show the DataFrame.   truncate=False: displays data at full length.
Customer_Account_Parsed_Address.show(truncate=False)

#prints all lines of data
#Customer_Account_Parsed_Address.show(Customer_Account_Parsed_Address.count(), truncate=False)
