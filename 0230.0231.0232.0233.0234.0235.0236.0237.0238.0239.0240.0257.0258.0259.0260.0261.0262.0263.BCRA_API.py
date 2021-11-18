#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


series_url = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/es_series.txt"
df_desc = pd.read_csv(series_url, delimiter=";", encoding="Latin-1", header=None)
df_desc


# In[3]:


df_desc.columns = ["Serie_id", "Variable_name", "Obs", "Obs2", "temp", "Frequency" ]


# In[9]:


datasets = {
        230: {
                "db_name": "Panorama monetario y financiero \(series mensuales\) - ",
                "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/panser.txt",
                "datasetName" : "Monetary - Argentina - BCRA - Panorama monetario y financiero"
            },
        231: {
                "db_name": 'Dinero y crédito - Balance consolidado del sistema financiero, saldos a fin de mes, en miles de pesos - ',
                "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
                "datasetName" : "Monetary - Argentina - BCRA - Balance consolidado del sistema financiero, saldos a fin de mes, en miles de pesos"
            },
        232:  {
                "db_name": 'Dinero y crédito - Balance de Banco Central de la República Argentina, en miles de pesos - Series mensuales, saldos a fin de mes - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
                "datasetName" : "Monetary - Argentina - BCRA - Balance de Banco Central de la República Argentina, en miles de pesos - Series mensuales, saldos a fin de mes"
              },
        233: {
                "db_name": 'Dinero y crédito - Balance consolidado de las entidades financieras, en miles de pesos - Series mensuales, saldos a fin de mes - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
                "datasetName" : "Monetary - Argentina - BCRA - Balance consolidado de las entidades financieras, en miles de pesos - Series mensuales, saldos a fin de mes"
            },    
        235: {
                "db_name": 'Dinero y crédito - Información relacionada con la normativa de regulación de liquidez del BCRA \(serie mensual\) - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
                "datasetName" : "Monetary - Argentina - BCRA - Información relacionada con la normativa de regulación de liquidez del BCRA (serie mensual)"
            },    
        236:  {
                     "db_name": "Dinero y crédito - Balance de Banco Central de la República Argentina, en miles de pesos - Series diarias - ",
                     "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din2_ser.txt",
                    "datasetName" : "Monetary - Argentina - BCRA - Balance de Banco Central de la República Argentina, en miles de pesos - Series diarias"
                   },            
           237: {
                    "db_name": 'Dinero y crédito - Balance consolidado de las entidades financieras, en miles de pesos - Series diarias - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din3_ser.txt",
                    "datasetName" : "Monetary - Argentina - BCRA - Principales activos de las entidades financieras  - Series diarias"
                },
           238: {
                    "db_name": 'Dinero y crédito - Balance consolidado de las entidades financieras, en miles de pesos - Series diarias - Total de depósitos y obligaciones - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din4_ser.txt",
                   "datasetName" : "Monetary - Argentina - BCRA -  Principales pasivos de las entidades financieras - Total - Series diarias"
                },
           239: {
                    "db_name": 'Dinero y crédito - Balance consolidado de las entidades financieras, en miles de pesos - Series diarias - Depósitos y obligaciones de y con el sector público - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din4_ser.txt",
                    "datasetName" : "Monetary - Argentina - BCRA - Principales pasivos de las entidades financieras - Con el sector privado - Series diarias"
                } ,
           240: {
                    "db_name": 'Dinero y crédito - Balance consolidado de las entidades financieras, en miles de pesos - Series diarias - Depósitos y obligaciones de y con el sector privado - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din4_ser.txt",
               "datasetName" : "Monetary - Argentina - BCRA - Principales pasivos de las entidades financieras - Con el sector publico - Series diarias"
                }  ,
        234: {
                "db_name": 'Dinero y crédito - Otras informaciones sobre la actividad financiera - Balance consolidado de bancos, saldos a fin de mes, en miles de pesos - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Otra informacion - Balance consolidado de bancos, en miles de pesos, saldos a fin de mes"
                },
        257: {
                "db_name": 'Dinero y crédito - Otras informaciones sobre la actividad financiera - Operaciones a futuro, saldos a fin de mes, en miles de pesos - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Otra informacion - Operaciones a futuro, saldos a fin de mes, en miles de pesos"
                },
        258: {
                "db_name": 'Dinero y crédito - Otras informaciones sobre la actividad financiera - Estado de situación de deudores, saldos a fin de mes - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Otra informacion - Estado de situación de deudores, saldos a fin de mes"
                },
        259: {
                "db_name": 'Dinero y crédito - Otras informaciones sobre la actividad financiera - Cuadro de resultados - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Otra informacion - Cuadro de resultados"
                },
        260: {
                "db_name": 'Dinero y crédito - Otras informaciones sobre la actividad financiera - Datos físicos \(cantidad de cuentas, titulares, personal y de entidades en actividad\)\(a fin de mes\/trimestre\) - ',
                "db_url" : "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Otra informacion - Datos físicos"
                },    
        262    : {
                    "db_name": 'Tasas de interés - Por depósitos - Series diarias - Tasas de interés por depósitos en caja de ahorros común y a plazo fijo, en porcentaje nominal anual - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/tas1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Tasas de interés - Por depósitos en caja de ahorros común y a plazo fijo"
            },
        263    : {
                    "db_name": 'Tasas de interés - Por depósitos - Series diarias - BADLAR - Tasas de interés por depósitos a plazo fijo de 30 a 35 días de plazo - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/tas1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Tasas de interés - BADLAR"
            },
        264    : {
                    "db_name": 'Tasas de interés - Por depósitos - Series diarias - Tasas de interés por depósitos en caja de ahorros común y a plazo fijo - ',
                    "db_url": "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/tas1_ser.txt",
            "datasetName" : "Monetary - Argentina - BCRA - Tasas de interés - Por depósitos en caja de ahorros común y a plazo fijo - por tipo de entidad"
            }    
            
                
    
    
    }


# In[11]:


for db_id in datasets:
    df = pd.read_csv(datasets[db_id]["db_url"], delimiter=";", encoding="Latin-1", header=None)
    df.columns = ["Serie_id", "Date", "Value"]
    df["Serie_id"] = pd.to_numeric(df["Serie_id"], errors="coerce")
    df = df.merge(df_desc, how= "left", left_on="Serie_id", right_on="Serie_id")
    df = df[df["Variable_name"].notnull()]
    df = df[df["Variable_name"].str.contains(datasets[db_id]["db_name"])]
    df["Variable_name"] = df["Variable_name"].str.replace(datasets[db_id]["db_name"], "")
    df["Date"] = df["Date"].str.replace("\'","")
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    del df["Obs"]
    del df["Obs2"]
    del df["temp"]
    del df["Serie_id"]
    del df["Frequency"]
    df = df.drop_duplicates(subset=["Date", "Variable_name"])
    df = df.set_index(["Date", "Variable_name"]).unstack()
    df.columns = df.columns.map(' - '.join)
    for col in df.columns:
        df = df.rename(columns={col: col.replace("Value - ", "")})
    df["country"] = "Argentina"       
    
    alphacast.datasets.dataset(db_id).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




