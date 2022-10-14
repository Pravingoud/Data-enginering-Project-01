#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import pandas as pd


# In[2]:


try: 
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=sqlsql12")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)


# In[3]:


try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)


# In[4]:


conn.set_session(autocommit=True)


# In[5]:


try: 
    cur.execute("create database newdb")
except psycopg2.Error as e:
    print(e)


# In[6]:


try: 
    conn.close()
except psycopg2.Error as e:
    print(e)
    
try: 
    conn = psycopg2.connect("host=localhost dbname=newdb user=postgres password=sqlsql12")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
    
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

conn.set_session(autocommit=True)


# In[7]:


customer_data = pd.read_csv(r"C:\dataset\Data\olist_customers_dataset.csv")


# In[8]:


customer_data.head()


# In[9]:


customer_data_clean = customer_data[['customer_id','customer_zip_code_prefix','customer_city','customer_state']]


# In[10]:


customer_data_clean.head()


# In[11]:


payment_data = pd.read_csv(r"C:\dataset\Data\olist_order_payments_dataset.csv")


# In[12]:


payment_data.head()


# In[13]:


seller_data = pd.read_csv(r"C:\dataset\Data\olist_sellers_dataset.csv")


# In[14]:


seller_data_clean = seller_data[['seller_id','seller_zip_code_prefix','seller_state']]


# In[15]:


customer_data_create_table = ("""CREATE TABLE customer (
customer_id VARCHAR PRIMARY KEY,
customer_zip_code INTEGER, 
customer_city VARCHAR,
seller_state VARCHAR)""")


# In[16]:


cur.execute(customer_data_create_table)
conn.commit()


# In[17]:


customer_data_insert = ("""INSERT INTO customer (
customer_id, 
customer_zip_code,
customer_city,
seller_state )
VALUES(%s,%s,%s,%s)""")


# In[18]:


for i, row in customer_data_clean.iterrows():
    cur.execute(customer_data_insert,list(row))


# In[19]:


payment_data_create_table = ("""CREATE TABLE payment (
order_id VARCHAR,
payment_sequential SMALLINT, 
payment_type VARCHAR,
payment_installments SMALLINT,
payment_value FLOAT(2))""")


# In[20]:


cur.execute(payment_data_create_table)
conn.commit()


# In[21]:


payment_data_insert = ("""INSERT INTO payment (
order_id, 
payment_sequential ,
payment_type,
payment_installments,
payment_value)
VALUES(%s,%s,%s,%s,%s)""")


# In[22]:


for i, row in payment_data.iterrows():
    cur.execute(payment_data_insert,list(row))


# In[23]:


seller_data_create_table = ("""CREATE TABLE seller (
seller_id VARCHAR PRIMARY KEY,
seller_zip_code INTEGER,
seller_state VARCHAR
)""")


# In[24]:


cur.execute(seller_data_create_table)
conn.commit()


# In[25]:


seller_data_insert = ("""INSERT INTO seller (
seller_id, 
seller_zip_code,
seller_state)
VALUES(%s,%s,%s)""")


# In[26]:


for i, row in seller_data_clean.iterrows():
    cur.execute(seller_data_insert,list(row))


# In[ ]:




