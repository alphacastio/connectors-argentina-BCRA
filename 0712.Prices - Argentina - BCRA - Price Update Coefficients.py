#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import numpy as np
import pandas as pd
from datetime import date, timedelta
from functools import reduce

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[ ]:


#Determino el periodo de informacion que se solicita
#5 dias previos hasta el 20 del mes siguiente
fecha_desde = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')

fecha_hasta = date(date.today().year, (date.today().month + 1), 20).strftime('%Y-%m-%d')


# In[ ]:


#Los codigos de los coeficientes son:
# CER 3540
# UVA 7913
# UVI 7914
# ICL 7988

dict_coeficientes ={
    '3540': ['Date', 'CER'],
    '7913': ['Date', 'UVA'],
    '7914': ['Date', 'UVI'],
    '7988': ['Date', 'ICL']
}

data_frames = []


# In[ ]:


#Para la actualizacion diaria, lo que se hace es 1 request por codigo de coeficiente, que trae
#la informacion de los 5 días previos hasta el dia 20 del mes siguiente

#La solicitud es con el metodo post y los nombres de las columnas ya vienen definidos en el diccionario de coeficientes

#Itero sobre los componentes del diccionario
for clave, valores in dict_coeficientes.items():
    #Armo la post data, al parecer el hay un par de parametros que figuran 2 veces, mantenemos la primera solamente
    post_data = {'fecha_desde': fecha_desde,
                 'fecha_hasta': fecha_hasta,
                 'B1':'Enviar',
                 'primeravez':'1',
                 #'fecha_desde':'20210929',
                 #'fecha_hasta':'20211101',
                 'serie': clave,
                 'serie1':'',
                 'serie2':'',
                 'serie3':'',
                 'serie4':'',
                 'detalle':''}
    
    #Hago el request post con el diccionario
    response = requests.post('http://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_datos.asp', 
                             data=post_data)
    
    #Luego de descargar la pagina, la leo con pandas, indicando el simbolo de decimal y de separador de miles
    df_temp = pd.read_html(response.content, thousands='.', decimal=',')[0]
    #Cambio el nombre de las columnas en base a la lista de cada clave
    df_temp.columns = valores
    
    #Paso a formato fecha la columna con las fechas diarias
    df_temp['Date'] = pd.to_datetime(df_temp['Date'], format='%d/%m/%Y')
    #seteo el indice
    df_temp.set_index('Date', inplace=True)
    
    #Guardo el dataframe en una lista
    data_frames.append(df_temp)


# In[ ]:


#Se aplica un reduce sobre la lista de dataframes (data_frames en este caso), de manera de fusionarlos a partir del indice
#se utiliza un outer por si faltan datos para algun caso
df = reduce(lambda left,right: pd.merge(left,right,left_index=True, right_index=True, 
                                        how='outer'), data_frames)


# In[ ]:


df['country'] = 'Argentina'


alphacast.datasets.dataset(7970).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:


#Esto sirvio para la creacion del dataset
#Trae todo el historico
# url_cer = 'http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/diar_cer.xls'
# url_uva = 'http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/diar_uva.xls'
# url_uvi = 'http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/diar_uvi.xls'
# url_icl = 'http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/diar_icl.xls'


# In[ ]:


# response_cer = requests.get(url_cer)
# response_uva = requests.get(url_uva)
# response_uvi = requests.get(url_uvi)
# response_icl = requests.get(url_icl)


# In[ ]:


# data_frames = []


# In[ ]:


##Se mantiene esto por si es necesario descargar todo el historico de nuevo

# df_cer = pd.read_excel(response_cer.content, skiprows=26)
# df_cer.columns = ['Date', 'CER']

# df_cer['Date'] = pd.to_datetime(df_cer['Date'], format = '%d/%m/%Y')
# df_cer.set_index('Date', inplace=True)
# data_frames.append(df_cer)


# In[ ]:


# df_uva = pd.read_excel(response_uva.content, skiprows=26)
# df_uva.columns = ['Date', 'UVA']

# df_uva['Date'] = pd.to_datetime(df_uva['Date'], format = '%d/%m/%Y')
# df_uva.set_index('Date', inplace=True)
# data_frames.append(df_uva)


# In[ ]:


# df_uvi = pd.read_excel(response_uvi.content, skiprows=26)
# df_uvi.columns = ['Date', 'UVI']

# df_uvi['Date'] = pd.to_datetime(df_uvi['Date'], format = '%d/%m/%Y')
# df_uvi.set_index('Date', inplace=True)
# data_frames.append(df_uvi)


# In[ ]:


# df_icl = pd.read_excel(response_icl.content, skiprows=26)
# df_icl.columns = ['Date', 'ICL']

# df_icl['Date'] = pd.to_datetime(df_icl['Date'], format = '%d/%m/%Y')
# df_icl.set_index('Date', inplace=True)
# data_frames.append(df_icl)


# In[ ]:


# df = reduce(lambda left,right: pd.merge(left,right,left_index=True, right_index=True, 
#                                         how='outer'), data_frames)


# In[ ]:


# df['country'] = 'Argentina'

