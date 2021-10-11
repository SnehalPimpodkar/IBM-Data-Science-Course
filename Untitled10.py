#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()

import pyspark

from pyspark.sql import functions as sf

sc = pyspark.SparkContext()

sqlc = pyspark.SQLContext(sc)

df = sqlc.createDataFrame([('Park','Jimin'), ('Kim','Namjoon')], ['Last_name', 'First_Name'])

df.show()


# In[3]:


df = df.withColumn('Full_name', 

                    sf.concat(sf.col('First_Name'),sf.lit('_'), sf.col('Last_name')))

df.show()


# In[ ]:




