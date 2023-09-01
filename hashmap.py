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

c.execute("select * from items")
i=c.fetchall()
dfitems = pd.DataFrame(i, columns=["item_id","prod_name", "description", "quantity","price"])

c.execute("select * from orders")
o=c.fetchall()
dforders = pd.DataFrame(o, columns=["order_id","user_id", "item_id","date"])





# def custom_merge_with_hashmap(df1, df2, merge_column):
#     # Create hashmaps with merge_column values as keys and lists of rows as values for both DataFrames
#     index_hashmap1 = {}
#     for _, row in df1.iterrows():
#         key = row[merge_column]
#         index_hashmap1.setdefault(key, []).append(row.to_dict())
#
#     index_hashmap2 = {}
#     for _, row in df2.iterrows():
#         key = row[merge_column]
#         index_hashmap2.setdefault(key, []).append(row.to_dict())
#
#     # Initialize an empty list to store merged rows
#     merged_data = []
#
#     # Iterate through the unique merge keys found in either dataframe
#     for key in index_hashmap1.keys() | index_hashmap2.keys():
#         rows_hashmap1 = index_hashmap1.get(key, [])
#         rows_hashmap2 = index_hashmap2.get(key, [])
#
#         for row1 in rows_hashmap1:
#             for row2 in rows_hashmap2:
#                 merged_row = {**row1, **row2}
#                 merged_data.append(merged_row)
#
#     # Create a new DataFrame from the merged data
#     merged_df = pd.DataFrame(merged_data)
#     return merged_df


def custom_merge_with_hashmap(df1, df2, merge_column):
    # Create hashmaps with merge_column values as keys and lists of rows as values for both DataFrames
    index_hashmap1 = {}
    for _, row in df1.iterrows():
        key = row[merge_column]
        index_hashmap1.setdefault(key, []).append(row.to_dict())

    index_hashmap2 = {}
    for _, row in df2.iterrows():
        key = row[merge_column]
        index_hashmap2.setdefault(key, []).append(row.to_dict())

    # Initialize an empty list to store merged rows
    merged_data = []

    # Iterate through the keys of the first hashmap and check for existence in the second hashmap
    for key in index_hashmap1:
        rows_hashmap1 = index_hashmap1[key]
        rows_hashmap2 = index_hashmap2.get(key, [])

        for row1 in rows_hashmap1:
            for row2 in rows_hashmap2:
                merged_row = {**row1, **row2}
                merged_data.append(merged_row)

    # Create a new DataFrame from the merged data
    merged_df = pd.DataFrame(merged_data)
    return merged_df


start = time.time()

merger = custom_merge_with_hashmap(dfitems, dforders, merge_column='item_id')
final_merged_result = custom_merge_with_hashmap(merger, dfusers, merge_column='user_id')
print(final_merged_result[['user_id','prod_name','email','name']])
# final_merged_result_sorted = final_merged_result.sort_values(by='user_id')
# print(final_merged_result_sorted[['user_id','prod_name','email','name']])
end = time.time()
print(end - start)