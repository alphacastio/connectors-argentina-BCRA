#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime

import re
from tqdm import tqdm
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url = 'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_cal.xls'

r = requests.get(url, allow_redirects=False, verify=False)

xl = pd.ExcelFile(url)
sheetnames= xl.sheet_names


# In[3]:


dfFinal = pd.DataFrame([])
for indice,sheet_name in enumerate(sheetnames):
#     with io.BytesIO(r.content) as datafr:
#         df = pd.read_excel(datafr, sheet_name=sheet_name, skiprows=17)
#     variableIndex= df[df[df.columns[0]] == 'fin000'].index[0]

#     df.columns = df.iloc[variableIndex]

#     beginIndex = variableIndex + 2
    
#     df.iloc[variableIndex].fillna('fin')
    
#     with io.BytesIO(r.content) as datafr:
    df = pd.read_excel(r.content, sheet_name=sheet_name, skiprows=17)

    # Codigo que fusiona las columnas que estan separadas en varias filas y les borra los guiones consecutivos
    columns = df[:7].fillna('')
    temp_cols=[]
    for i in range(0,columns.shape[1]):
        #El primer valor de la columna es el codigo identificador que tiene al worksheet del banco central
        coltemp = [str(df.iloc[7,i])]
        for j in range(0, columns.shape[0]):
            value = columns.iloc[j,i]
            coltemp += [value]
        temp_cols+=['-'.join(coltemp)]

    newColumns = []
    
    for col in temp_cols:
        col = re.sub(r"(-){2,}", "-", col)

        if len(col) > 0:
            if col[0] == '-':
                col = col[1:]               
            elif col[-1] == '-':
                col = col[:-1]

#         print(col)
        newColumns += [col]

    df.columns = newColumns
    df = df[9:] 
    
    
    
    df = df.dropna(how='all', subset= df.columns[1:])
    df.reset_index()
    def change_date(x):
        list_x= x.split('/')
        date = list_x[2]+'-'+list_x[1]+'-'+list_x[0]
        return date
    
    df['Date'] = df[df.columns[0]].apply(lambda x: change_date(x))
    del df[df.columns[0]]
    df= df.set_index('Date')
    if indice==0:
        dfFinal = df
    else:
        dfFinal = dfFinal.merge(df, how='left', left_index=True, right_index=True)


# In[4]:


for col in dfFinal.columns:
    if col != "country":
        #print(col)
        dfFinal[col] = pd.to_numeric(dfFinal[col], errors="coerce")
        
dfFinal['country'] = 'Argentina'


alphacast.datasets.dataset(123).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




