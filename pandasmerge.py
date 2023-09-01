import mysql.connector
import pandas as pd
import time
start=time.time()

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

handle = pd.merge(dfitems,dforders,on='item_id', how='inner')
final_merge= pd.merge(handle,dfusers, on='user_id',how='inner')
print(final_merge[['user_id','prod_name','email','name']].sort_values(by='user_id'))
end = time.time()
print(end - start)