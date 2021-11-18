#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install lxml 
import pandas as pd
import requests, json
import numpy as np
# from urllib.request import urlopen
from lxml import etree
from collections import OrderedDict

from tqdm import tqdm
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


# reading  Codificaciones sheet
file_url = "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Anexo.xls"
df_Codificaciones = pd.read_excel(file_url, sheet_name="Codificaciones", skiprows=3,header=[0])
df_Codificaciones


# In[3]:


df_ANEXO = df_Codificaciones[['ANEXO', 'Significado']] 
  
json_ANEXO = {}
for i, item in enumerate(df_ANEXO.to_dict('list')['ANEXO']):
    json_ANEXO[str(item)] = df_ANEXO.to_dict('list')['Significado'][i]

df_CV = df_Codificaciones[['C-V', 'Significado.1']] 
  
json_CV = {}
for i, item in enumerate(df_CV.to_dict('list')['C-V']):
    if str(item) == 'nan':
        continue
    json_CV[str(item)] = df_CV.to_dict('list')['Significado.1'][i]
json_CV


# In[4]:


file_url = "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Anexo.xls"
df = pd.read_excel(file_url, sheet_name="Datos", skiprows=3,header=[0])
df


# In[5]:


df = df.astype({"ANEXO": str})
for index, row in df.iterrows():
    val_ANEXO = row['ANEXO']
    df.at[index,'ANEXO']=json_ANEXO[str(val_ANEXO)]
    
    val_CV = row['C-V']
    df.at[index,'C-V']=json_CV[str(val_CV)]
#     break
df['Años']= df['Años'].astype(str)

# df['Date'] = df['Años'] + '-12-1'
# del df['Años']

df['country'] = 'Argentina'


# In[ ]:





# In[6]:


df.head()


# In[7]:


dfFinal = pd.DataFrame([])
for i in tqdm(range(1,13), desc='Looping over months'):
    column = str(i)
    temp = df.copy()
    fullcolumn = ''
    if len(column) == 1:
        fullcolumn = '0'+column
    else:
        fullcolumn = column
        
    temp['Date'] = temp['Años']+'-'+fullcolumn+'-01'
    del temp['Años']
    temp['country'] = temp['Denominación']+' - '+ temp['C-V']
    del temp['Denominación']
    del temp['C-V']
    temp_pivot = pd.pivot_table(temp, values=column, index=['Date', 'country'],
                                columns=['ANEXO'], aggfunc=np.sum)
    dfFinal = dfFinal.append(temp_pivot)
    
    

dfFinal = dfFinal.replace('-', np.nan)
# dfFinal['country'] = 'Argentina'


# In[8]:


# dfFinal =dfFinal.reset_index(level=-1, drop=True).reset_index()
# dfFinal = dfFinal.set_index('Date')
# dfFinal


# In[9]:


# dfFinal = dfFinal.reset_index(level=-1, drop=False).reset_index()
# .set_index("Date")


# In[10]:


# dfFinal = dfFinal.rename(columns= {'index': 'Date'})
# dfFinal[dfFinal.columns[1:]]
dfFinal.index.names


# In[11]:


# def multi_index_to_single(df):
#     indice = dfFinal.index
#     indice = list(indice)
#     Date = []
#     Entity = []
#     for i in range(0, len(indice)):
#         Date += [indice[i][0]]
#         Entity += [indice[i][1]]
#     Date = pd.Series(Date)
#     Entity = pd.Series(Entity)

#     return Date, Entity


# In[12]:


# dfFinal = dfFinal.reset_index()
# if isinstance(dfFinal.columns, pd.MultiIndex):
#     dfFinal.columns = dfFinal.columns.map(' - '.join)
# dfFinal = dfFinal.set_index('Date')
# dfFinal


# In[13]:


list_columns = list(dfFinal.columns)
newCols = []
for col in list_columns:
    newcol = col.strip()
    newCols +=[newcol]
    
dfFinal.columns = newCols


# In[14]:


# index_list=list(dfFinal.index)

# date=[]
# country=[]
# for i,element in enumerate(index_list):
#     date+=[index_list[i][0]]
#     country+=[index_list[i][1]]

# date = pd.Series(date)
# country = pd.Series(country)


# In[15]:


dfFinal = dfFinal.reset_index()
dfFinal = dfFinal.replace(0, np.nan)
dfFinal = dfFinal.replace('0', np.nan)
dfFinal = dfFinal.dropna(how='all', subset= dfFinal.columns[1:])
dfFinal
dfFinal = dfFinal.set_index('Date')


# In[19]:


dfFinal.index = pd.to_datetime(dfFinal.index, format="%Y-%m-%d")

alphacast.datasets.dataset(121).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[18]:





# In[ ]:




