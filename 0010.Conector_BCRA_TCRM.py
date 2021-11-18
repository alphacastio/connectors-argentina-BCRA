import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

filename = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xls"
skiprows = 1
header = [0]

sheet_name = "ITCRM y bilaterales"
df_merge = pd.read_excel(filename, sheet_name=sheet_name, skiprows= skiprows, header=header)
df_merge.dropna(axis=0, how='all',inplace=True)
df_merge.dropna(axis=1, how='all',inplace=True)
df_merge["Date"] = pd.to_datetime(df_merge["Período"], errors= "coerce", format= "%Y/%m/%d %H:%M:%S")
df_merge = df_merge[df_merge["Date"].notnull()]
df_merge = df_merge.set_index("Date")
del df_merge["Período"]
df_merge

for column in df_merge.columns:
    df_merge = df_merge.rename(columns={column: column.strip()})

df_merge["country"] = "Argentina"

del df_merge["Mes de referencia"]


alphacast.datasets.dataset(10).upload_data_from_df(df_merge, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



