#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url = 'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_bas.xls'

r = requests.get(url, allow_redirects=False, verify=False)


# In[7]:


df = pd.read_excel(r.content, skiprows=15)

if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.map(' - '.join)


# In[8]:


columns = df[:8]
columns = columns.fillna('')
temp_cols=[]
for i in range(0,columns.shape[1]):
    coltemp = []
    for j in range(0, columns.shape[0]):
        value = columns.iloc[j,i]
        coltemp += [value]
    
    temp_cols+=['-'.join(coltemp)]
    
newColumns = []

i=0
j=0
k=0
for col in temp_cols:
    
    col = re.sub(r"(-){2,}", "-", col)
    
    if len(col)>0:
        if col[0] == '-':
            col = col[1:]
        elif col[-1] == '-':
            col = col[:-1]
    
    if col =='Monto-efectivo-colocado-':
        col=col+str(i)
        i+=1
    
    if col == 'Monto-venci-miento-':
        col=col+str(k)
        k+=1
    
    if col == 'Fondo de-liquidez-bancaria-':
        col = col+str(j)
        j+=1
    
    newColumns += [col]
    

df.columns = newColumns
df = df[11:]
# df


# In[9]:


df = df.dropna(how='all', subset= df.columns[1:])
df.reset_index()

# df.info()

def date_maker(x):
    list_x = x.split('/')
    newDate = list_x[2]+'-'+list_x[1]+'-'+list_x[0]
    return newDate

df['Date'] = df[df.columns[0]].apply(lambda x: date_maker(x))
del df[df.columns[0]]


df = df.set_index('Date')

for col in df.columns:
    if col != "country":
        #print(col)
        df[col] = pd.to_numeric(df[col], errors="coerce")

df['country'] = 'Argentina'


alphacast.datasets.dataset(119).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




