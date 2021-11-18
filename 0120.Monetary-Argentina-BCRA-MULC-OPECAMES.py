#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime

from tqdm import tqdm
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


def take_year(x):
    if x[:9] == 'ACUMULADO':
        list_x = x.split(' ')
        x= list_x[1]
    else:
        x=np.nan
    
    return x

def replace_month(x):
    if x=='ENERO':
        x= x.replace('ENERO', '01-01')
    elif x=='FEBRERO':
        x= x.replace('FEBRERO', '02-01')
    elif x=='MARZO':
        x= x.replace('MARZO', '03-01')
    elif x=='ABRIL':
        x= x.replace('ABRIL', '04-01')
    elif x=='MAYO':
        x= x.replace('MAYO', '05-01')
    elif x=='JUNIO':
        x= x.replace('JUNIO', '06-01')
    elif x=='JULIO':
        x= x.replace('JULIO', '07-01')
    elif x=='AGOSTO':
        x= x.replace('AGOSTO', '08-01')
    elif x=='SEPTIEMBRE':
        x= x.replace('SEPTIEMBRE', '09-01')
    elif x=='OCTUBRE':
        x= x.replace('OCTUBRE', '10-01')
    elif x=='NOVIEMBRE':
        x= x.replace('NOVIEMBRE', '11-01')
    elif x=='DICIEMBRE':
        x= x.replace('DICIEMBRE', '12-01')
    else:
        x= np.nan
    return x


# In[3]:


url = 'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/opecames.xls'
r = requests.get(url, allow_redirects=False, verify=False)


xl = pd.ExcelFile(url)
sheetnames= xl.sheet_names[:-1]

dfFinal = pd.DataFrame([])

for i, sheet in tqdm(enumerate(sheetnames), desc='Looping over sheets'):
    with io.BytesIO(r.content) as datafr:
        df = pd.read_excel(datafr, skiprows=6, sheet_name=sheet)

    df = df.dropna(how='all', subset= df.columns[1:])


    df['year'] = df['PERÍODO'].apply(lambda x: take_year(x))
    df['year'] = df['year'].fillna(method='bfill')
    df['year'] = df['year'].astype(str)
    df['PERÍODO'] = df['PERÍODO'].apply(lambda x: replace_month(x))
    df = df[df['PERÍODO'].notnull()]
# #     del df['Unnamed: 7']
    df = df.dropna(how='all',axis=1)

    df['Date'] = df['year'] + '-'+ df['PERÍODO']
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
    
    del df['year']
    del df['PERÍODO']
#     del df['Item de memorandum: Estimación pagos locales por consumos con tarjetas en el exterior']
    df = df.set_index('Date')
    
    new_cols = []
    for col in df.columns:
        temp = sheet + ' - ' + col
        new_cols +=[temp]
    df.columns = new_cols
    
    
    
    if i==0:
        dfFinal = df
    else:        
        #continue
        dfFinal = dfFinal.merge(df, how='left', left_index=True, right_index=True)
    
        
dfFinal['country'] = 'Argentina'
dfFinal


# In[4]:


new_columns = []

for column in dfFinal.columns:
    column = column.strip()
    new_columns += [column]

dfFinal.columns = new_columns
dfFinal.head()


alphacast.datasets.dataset(120).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




