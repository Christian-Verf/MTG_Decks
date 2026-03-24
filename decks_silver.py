# Define the schema for the sales_silver table
    
from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("dbo.decks_silver") \
    .addColumn("deck", StringType()) \
    .addColumn("card", StringType()) \
    .addColumn("maindeck", StringType()) \
    .addColumn("count", IntegerType()) \
    .addColumn("Created_or_Modified_TS", TimestampType()) \
    .execute()