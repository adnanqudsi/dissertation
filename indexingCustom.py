import mysql.connector
import pandas as pd
import time
import cProfile

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

c.execute("select * from items")
i=c.fetchall()
dfitems = pd.DataFrame(i, columns=["item_id","prod_name", "description", "quantity","price"])

c.execute("select * from orders")
o=c.fetchall()
dforders = pd.DataFrame(o, columns=["order_id","user_id", "item_id","date"])


# def custom_merge(df1, df2, merge_column):
#     # Create dictionaries with merge_column values as keys and rows as values
#     index_dict1 = {row[merge_column]: row for _, row in df1.iterrows()}
#     index_dict2 = {row[merge_column]: row for _, row in df2.iterrows()}
#     print(len(index_dict1))
#     print(len(index_dict2))
#
#
def custom_merge_indexing(df1, df2, merge_column):
    # Create dictionaries with merge_column values as keys and lists of rows as values for both DataFrames
    index_dict1 = {key: df1[df1[merge_column] == key].to_dict(orient='records') for key in df1[merge_column]}
    index_dict2 = {key: df2[df2[merge_column] == key].to_dict(orient='records') for key in df2[merge_column]}

    # Initialize an empty list to store merged rows
    merged_data = []

    # Get all unique 'key' values from both DataFrames - union
    merge_keys = set(index_dict1.keys()) | set(index_dict2.keys())

    for key in merge_keys:
        # If the key exists in both DataFrames, then fetch the row values and merge the rows based on the common key
        if key in index_dict1 and key in index_dict2:
            rows_df1 = index_dict1[key]
            rows_df2 = index_dict2[key]
            for row1 in rows_df1: # a key can have multiple rows as values
                for row2 in rows_df2:
                    merged_row = {**row1, **row2} #dictionary unpacking- storing both the rows as one entity
                    merged_data.append(merged_row)

    # Create a new DataFrame from the merged data
    merged_df = pd.DataFrame(merged_data)
    return merged_df


start = time.time()
merger = custom_merge_indexing(dfitems, dforders, merge_column='item_id')
final_merged_result = custom_merge_indexing(merger, dfusers, merge_column='user_id')
print(final_merged_result[['user_id','prod_name','email','name']])

end = time.time()
print(end - start)