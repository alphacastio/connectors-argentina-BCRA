#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url = 'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_cre.xls'

r = requests.get(url, allow_redirects=False, verify=False)

with io.BytesIO(r.content) as datafr:
    df = pd.read_excel(datafr)

    
variableIndex= df[df[df.columns[0]] == 'cd_serie'].index[0]

df.columns = df.iloc[variableIndex]

beginIndex = variableIndex + 1
df= df[beginIndex:]
df = df.dropna(how='all', subset= df.columns[1:])
df.reset_index()

# df.info()

def date_maker(x):
    list_x = x.split('/')
    newDate = list_x[2]+'-'+list_x[1]+'-'+list_x[0]
    return newDate

df['Date'] = df['cd_serie'].apply( lambda x: date_maker(x))
del df['cd_serie']

df.columns = ['3731', 'Date']
df = df.set_index('Date')
df['country'] = 'Argentina'


# In[3]:


df


# In[5]:


df.index = pd.to_datetime(df.index, format="%Y-%m-%d")

alphacast.datasets.dataset(122).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




