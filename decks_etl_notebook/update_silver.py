# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dbo/decks_silver')
    
dfUpdates = df
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.deck = updates.deck and silver.card = updates.card and silver.maindeck = updates.maindeck'
  ) \
   .whenMatchedUpdate(set =
    {
      "maindeck": "updates.maindeck",
      "count": "updates.count",
      "Created_or_Modified_TS": "updates.Created_or_Modified_TS" 
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "deck": "updates.deck",
      "card": "updates.card",
      "maindeck": "updates.maindeck",
      "count": "updates.count",
      "Created_or_Modified_TS": "updates.Created_or_Modified_TS"
    }
  ) \
  .whenNotMatchedBySourceDelete() \
  .execute()