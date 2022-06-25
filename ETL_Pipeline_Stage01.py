#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#This an ETL pipeline for disaster data cleaning and preparation for an ML pipeline
# import libraries
import pandas as pd
import numpy as np


# In[ ]:


# load messages dataset
messages = pd.read_csv("C:\\Users\\debo_\\Desktop\\ML\\Projects_for_Udacity_Data_science\\ETL_Pipeline_disaster_response\\data\\disaster_messages.csv")
messages.head()


# In[ ]:


messages.shape


# In[ ]:


# load categories dataset
categories = pd.read_csv("C:\\Users\\debo_\\Desktop\\ML\\Projects_for_Udacity_Data_science\\ETL_Pipeline_disaster_response\\data\\disaster_categories.csv")
categories.head()


# In[ ]:


categories.shape


# In[ ]:


# merge datasets
df=pd.merge(messages, categories, on="id")
df


# In[ ]:


#Split categories into separate category columns.
# create a dataframe of the 36 individual category columns
#df_final['categories'].str.split(';', expand=True)
categories = df['categories'].str.split(';', expand=True)
categories.head()


# In[ ]:


row = categories.iloc[0]


# In[ ]:


# select the first row of the categories dataframe

row_1=categories.head(1)
row = categories.iloc[0]
categories.columns=row
# use this row to extract a list of new column names for categories.
# one way is to apply a lambda function that takes everything 
# up to the second to last character of each string with slicing
category_colnames = categories.columns
print(category_colnames)


# In[ ]:


# rename the columns of `categories`
categories.columns = category_colnames
categories.head()


# In[ ]:


#Convert category values to just numbers 0 or 1
for column in categories:
    # set each value to be the last character of the string
    categories[column] = df.categories.astype(str).str[-1:]
    
# convert column from string to numeric
categories[column] =  categories[column].astype(np.int64)
categories


# In[ ]:


#Replace categories column in df with new category columns.
# drop the original categories column from `df`
df.drop('categories', axis=1, inplace=True)
df.head()


# In[ ]:


# concatenate the original dataframe with the new `categories` dataframe 
df = pd.concat([df, categories], axis =1)
df
#df.to_csv("C:\\Users\\debo_\\Desktop\\ML\\Projects_for_Udacity_Data_science\\ETL_Pipeline_disaster_response\\data\\disasterC2.csv")


# In[ ]:


#Remove duplicates.
# check number of duplicates
duplicateRows = df[df.duplicated()]
duplicateRows


# In[ ]:


#to display the first duplicate rows instead of the last
duplicateRows_first = df[df.duplicated(keep='last')]
duplicateRows_first


# In[ ]:


# drop duplicates
df2 = df.drop_duplicates(keep='first')
df2


# In[ ]:


# check number of duplicates again
duplicaterows = df2[df2.duplicated()]
duplicaterows


# In[ ]:


#Save the clean dataset into an sqlite database
import sqlite3 as db
from sqlalchemy import create_engine
engine = create_engine('sqlite:///clean_disasterC.db')
df2.to_sql('MCat', engine, index=False)

