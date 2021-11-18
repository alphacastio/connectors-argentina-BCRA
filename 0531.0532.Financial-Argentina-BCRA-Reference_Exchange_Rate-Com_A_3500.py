#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests

from datetime import datetime
from urllib.request import urlopen
from lxml import etree
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[2]:


url = "http://www.bcra.gov.ar/PublicacionesEstadisticas/Tipos_de_cambios.asp"
response = requests.get(url,verify=False)
html = response.content
htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
xls_address = tree.xpath("/html/body/div/div[2]/div/div/div/ul/li[4]/a/@href")[0]


# In[3]:


url = 'http://www.bcra.gov.ar/' + xls_address
r = requests.get(url, allow_redirects=True, verify=False)


# In[4]:


df = pd.read_excel(r.content, skiprows=3, sheet_name='TCR diario y TCNPM')
df = df.dropna(how='all').dropna(how='all', axis=1)
df = df.rename(columns = {'Tipo de Cambio de Referencia - en Pesos - por Dólar': 'TC de Referencia ($/USD)','Fecha':'Date'})
df = df.drop('Tipo de Cambio Nominal Promedio Mensual', axis=1)
df['Date'] = pd.to_datetime(df['Date'])
df = df.set_index('Date')
df['country'] = 'Argentina'


# In[5]:


def fix_date(df):
    df["DateOk"] = pd.to_datetime(df["Date"], errors="coerce")
    df_to_fix = df[df["DateOk"].isnull()]
    df_to_fix["Date"] = df_to_fix["Date"].str.replace("*", "")

    df_to_fix["year"] = "20" + df_to_fix["Date"].str.split("-").str[1]
    df_to_fix["month"] = df_to_fix["Date"].str.split("-").str[0]
    df_to_fix["day"] = 1
    df_to_fix["month"] = df_to_fix["month"].str.lower()
    df_to_fix["month"] = df_to_fix["month"].replace(
        {
            "enero": "01",
            "febrero": "02",
            "marzo": "03",
            "abril": "04",
            "mayo": "05",
            "junio": "06",
            "julio": "07",
            "agosto": "08",
            "septiembre": "09",
            "octubre": "10",
            "noviembre": "11",
            "diciembre": "12",

        })
    df_to_fix["DateOk"] = pd.to_datetime(df_to_fix[["year", "month", "day"]])
    df_fix = df[df["DateOk"].notnull()].append(df_to_fix)
    return df_fix


# In[6]:


df1 = pd.read_excel(r.content, skiprows=1, sheet_name='Serie de TCNPM')
df1 = df1.dropna(how='all').dropna(how='all', axis=1)
df1 = df1.rename(columns={'Mes':'Date','Tipo de cambio nominal promedio mensual':'TC Nominal Promedio Mensual'})

df1 = fix_date(df1)
df1 = df1.drop(['year','month','day','Date'], axis=1)
df1 = df1.rename(columns={'DateOk':'Date'})
df1 = df1.set_index('Date')
df1.sort_index(inplace=True)
df1['country'] = 'Argentina'


alphacast.datasets.dataset(531).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(532).upload_data_from_df(df1, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




