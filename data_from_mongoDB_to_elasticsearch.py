#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install requests



# In[26]:


pip install numpy


# In[28]:


pip install pandas


# In[29]:


import numpy as np
# import torch
import os
import pandas as pd
import time


# In[5]:


pip install elasticsearch


# In[2]:


from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")


# In[3]:


resp = es.info()


# In[4]:


resp


# In[7]:


es.indices.create(index="mydatabase",ignore=400)


# In[9]:


import elasticsearch


# In[12]:


es.index(index='my_index', id=1, body={'text': 'this is a test'})


# In[13]:


es.index(index='my_index', id=2, body={'text': 'a second test'})


# In[14]:


es.search(index='my_index', body={'query': {'match': {'text': 'this test'}}})


# In[15]:


es.search(index='mydatabase')


# In[16]:


data={
  "_id": {
    "$oid": "63a52ad2e45bcae2d46b70c3"
  },
  "Heading": "MHMR Authority Of Brazos Valley",
  "Date": "22 December 2022",
  "Content": "The MHMR Authority of Brazos Valley is a public non-profit community MHMR center. Through the Texas Department of State Health Services and Texas Department of"
}


# In[19]:


es.index(index='my_index', id=2, body={'text': 'aman'})


# In[20]:


get_ipython().system('pip install "pymongo[srv]"')


# In[186]:


from pymongo import MongoClient
def get_database():
   CONNECTION_STRING = "mongodb+srv://emseccomandcenter:TUXnEN09VNM1drh3@cluster0.psiqanw.mongodb.net/?retryWrites=true&w=majority"
   client = MongoClient(CONNECTION_STRING)
   return client['webScraping']


# In[187]:


db = get_database()
collection = db["rohan"]


# In[188]:


print ("total docs in collection:", collection.count_documents( {} ))


# In[189]:


dfmongo = pd.DataFrame(list(collection.find()))


# In[190]:


df=dfmongo.loc[:,'_i':'Content']
df


# In[191]:


n=0
def fun(a):
    global n
    n=n+1
    return n


# In[192]:


df['Id']=df['_id'].apply(fun)


# In[194]:


data1=df.loc[:,'Heading':'Id']


# In[195]:


data=data1.to_dict('records')


# In[196]:


data


# In[132]:


data.isnull().sum()


# In[207]:


from elasticsearch import Elasticsearch,helpers
def df_generator(data):
    for index,document in enumerate(data):
        yield {
            '_index':'data',
            '_type':'dataset',
            '_id':document.get('Id'),
            "_source":{
                'Heading':document.get('Heading',["NO data"]),
                'Date':document.get('Date'),
                'Content':document.get('Content')
                }
            }
    raise StopIteration


# In[208]:


gen=df_generator(data)


# In[209]:


gen


# In[210]:


next(gen)


# In[211]:


try:
    res=helpers.bulk(es,df_generator(data)) 
    print("working")
except Exception as e:
    print("Done")


# In[221]:


res=es.search(index='data', body={'query': {'match': {'Heading': 'The MHMR Authority of Brazos Valley is a public'}}})
res['hits']['hits']


# In[ ]:




