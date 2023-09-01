import inspect

import mysql.connector
from sqlalchemy import create_engine, ForeignKey,Column, String,Integer, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np
import time
start = time.time()

import pandas as pd
mydb= mysql.connector.connect(
    host="localhost",
    user="root",
    password="Adnan132001.",
    database="final"
)
c=mydb.cursor()
# c.execute(" select * from users")
# r=c.fetchall()
# dfusers = pd.DataFrame(r, columns=["user_id","name","email","registeration_date","address","gender","age"])
# print(dfusers.head())
# c.execute("select * from items")
# i=c.fetchall()
# dfitems = pd.DataFrame(i, columns=["item_id","prod_name", "description", "quantity","price"])
# print(dfitems.head())
# c.execute("select * from orders")
# o=c.fetchall()
# dforders = pd.DataFrame(o, columns=["id","user_id", "item_id","date"])
# print(dforders.head())
c.execute(" select users.name, users.email,users.id, items.name from orders inner join items on orders.item_id = items.id inner join users on orders.user_id=users.id;")
f=c.fetchall()
mergeDF = pd.DataFrame(f, columns=["name","email", "user_id","item_name"])
sortedfinal = mergeDF.sort_values(by='user_id')
print(sortedfinal)
end = time.time()
print(end - start)