#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[ ]:


page = requests.get('http://www.bcra.gov.ar/PublicacionesEstadisticas/Precios_materias_primas.asp')
soup = BeautifulSoup(page.content, 'html.parser')


# In[ ]:


link_xls=[]
for link in soup.find_all('a'):
    if 'Descargar la serie histórica' in link.get_text():
        link_xls.append('http://www.bcra.gov.ar' + link.get('href'))


# In[ ]:


#Reemplazo para que quede bien la url
link_xls[0] = link_xls[0].replace('..', '')


# In[ ]:


df = pd.read_excel(link_xls[0], sheet_name=1)
#Reemplazo el salto de linea en los nombres de las columnas
df.columns = df.columns.str.replace('\n', ' ')


# In[ ]:


#Renombro la primera columna, paso a formato fecha y elimino los NaN
df.rename(columns={df.columns[0]:'Date'}, inplace=True)

df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df.dropna(subset=['Date'], inplace=True)


# In[ ]:


#Esto es para que deje el primer dia del mes, en el original estaba el ultimo dia del mes
df['Date'] = df['Date'].dt.strftime('%Y-%m-01')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')


# In[ ]:


df.set_index('Date', inplace=True)
df['country'] = 'Argentina'


alphacast.datasets.dataset(7756).upload_data_from_df(df,entitiesColumnNames = ['country'],
                                deleteMissingFromDB = True, onConflictUpdateDB = True)

# In[ ]:




