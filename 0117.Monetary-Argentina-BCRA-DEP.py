#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import re
import io
from datetime import datetime
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:





# In[3]:


url = 'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_dep.xls'
r = requests.get(url, allow_redirects=False, verify=False)

xl = pd.ExcelFile(url)
sheet_names= xl.sheet_names



# In[4]:


sheet_names


# In[4]:


dfFinal = pd.DataFrame([])
for i,sheet_name in enumerate(sheet_names):
    #with io.BytesIO(r.content) as datafr:
    #    df = pd.read_excel(datafr, sheet_name=sheet_name)
    #variableIndex= df[df[df.columns[0]] == 'dep000'].index[0]

    #df.columns = df.iloc[variableIndex]

    #beginIndex = variableIndex + 2
    #df= df[beginIndex:]
    check = i==0
    print(check)
    with io.BytesIO(r.content) as datafr:
        df = pd.read_excel(datafr, sheet_name=sheet_name, skiprows=17)

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
    def date_maker(x):
        list_x = x.split('/')
        newDate = list_x[2]+'-'+list_x[1]+'-'+list_x[0]
        return newDate

    df['Date'] = df['dep000'].apply( lambda x: date_maker(x))
    del df['dep000']
#     print(df.columns)
    df= df.set_index('Date')
    print(df.shape)
    print(int(i)==1)
    if check == True:
        dfFinal = dfFinal.append(df)
        print('Las dimensiones de dfFinal ahora son: {}'.format(dfFinal.shape))
    else:
        dfFinal = pd.merge(dfFinal, df, how='left', left_index=True, right_index=True)
        print('Las dimensiones de dfFinal ahora son: {}'.format(dfFinal.shape))


dfFinal["country"] = "Argentina"


# In[5]:


dfFinal = dfFinal[dfFinal.index.duplicated()==False]


alphacast.datasets.dataset(117).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




