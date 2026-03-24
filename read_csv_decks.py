# Welcome to your new notebook
# Type here in the cell editor to add code!
# Schema definition & read csv
from pyspark.sql.types import *
    
# Create the schema for the table
orderSchema = StructType([
    StructField("deck", StringType()),
    StructField("maindeck", StringType()),
    StructField("count", IntegerType()),
    StructField("card", StringType())
    ])
    
# Import all files from bronze folder of lakehouse
df = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(orderSchema).load("Files/decks_bronze/*.csv")
    
# Display the first 10 rows of the dataframe to preview your data
display(df.head(10))