import mysql.connector
import pandas as pd
import time
mydb= mysql.connector.connect(
    host="localhost",
    user="root",
    password="Adnan132001.",
    database="final"
)
c=mydb.cursor()
c.execute(" select * from users")
r=c.fetchall()
dfusers = pd.DataFrame(r, columns=["user_id","name","email","registeration_date","address","gender","age"])
#print(len(dfusers))
c.execute("select * from items")
i=c.fetchall()
dfitems = pd.DataFrame(i, columns=["item_id","prod_name", "description", "quantity","price"])
#print(len(dfitems))

c.execute("select * from orders")
o=c.fetchall()
dforders = pd.DataFrame(o, columns=["order_id","user_id", "item_id","date"])
#print(len(dforders))

def custom_merge(df1, df2, merge_column):
    merged_data = []
    for a, row1 in df1.iterrows():
        for b, row2 in df2.iterrows():
            if row1[merge_column] == row2[merge_column]:
                # Merge the rows based on the common key
                merged_row = {**row1, **row2}
                #dictionary unpacking; if condition meets every item of row and row 2 is put together as one entitiy in new dictinry - merged_row in our case
                merged_data.append(merged_row)

    # Create a new DataFrame from the merged data
    merged_df = pd.DataFrame(merged_data)
    return merged_df

start=time.time()
merged_result = custom_merge(dfitems, dforders, merge_column='item_id')
final_merger = custom_merge(merged_result, dfusers, merge_column='user_id')
print(final_merger[['user_id','prod_name','email','name']])
end = time.time()
print(end - start)