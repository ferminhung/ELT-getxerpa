import json
import csv
import requests
from datetime import datetime 
from datetime import date
import numpy as np
from io import StringIO
import pandas as pd
import operator
import os
from json import loads
import gspread

GSHEETS_CREDENTIALS="getxerpa-16db85225cbf.json"

def guardarTabla(df, sheet_name, worksheet_name):
    #Procedimiento para transferir dataframa a una hoja de calculo en google sheets
    client = gspread.service_account(GSHEETS_CREDENTIALS)
    sheet = client.open(sheet_name)
    worksheet = sheet.add_worksheet(title=worksheet_name, rows=100, cols=20)
    #worksheet = sheet.worksheet(worksheet_name)

    # convertimos el tipo de las columnas que sean datetime a string
    for column in df.columns[df.dtypes == 'datetime64[ns]']:
        df[column] = df[column].astype(str)

    # reemplazamos valores NaN por strings vacíos
    worksheet.update([df.columns.values.tolist()] + df.fillna('').values.tolist())

    print(f'DataFrame escrito en la hoja {sheet_name} / {worksheet_name}.')

def crearDF(df,atrib):
    #Procedimiento para construir dataframe generico
    primero = df.head(1)
    ultimo = df.tail(1)
    cierre_inicial = primero['Close'].values[0]
    cierre_final = ultimo['Close'].values[0]
    print(cierre_inicial,cierre_final)
    metricas = ['intervalo',
                'max', 
                'percentil 75',
                'percentil 25',
                'min', 
                'promedio',
                'mediana',
                'coef. de var.',
                'cambio de precio',
                'volumen prom']
    valores = [atrib,
               df['High'].max(), 
               df['Close'].quantile(0.75).round(2), 
               df['Close'].quantile(0.25).round(2), 
               df['Low'].min(), 
               df['Close'].mean().round(2),
               df['Close'].median().round(2),
               df["Close"].std(ddof=0) / df["Close"].mean(),
               (cierre_final - cierre_inicial) / cierre_inicial, 
               df['Volume'].mean().round(0)]
    
    df = pd.DataFrame()
    df['metricas'] = metricas
    df['valores'] = valores

    return df

def crearDFAnual(df,atrib):
    #Procedimiento para crear dataframe anual, solo funciona con la fuente de datos de prueba
    df['Date']=df['Date'].astype(str)
    df[['ano','mes','dia']] = df['Date'].str.split('-', expand=True)

    df = df.groupby("ano")["Close"].aggregate(['max', 'min','mean'])
    return df

def bajardatos():
    #Procedimiento para descargar la informacion en .CSV desde el api de eodhistoricaldata
    print("Indica el Ticker -> ", end="")
    ticker = input()
    response = requests.get('https://eodhistoricaldata.com/api/eod/'+ticker+'.US?api_token=63f13355271d82.00446851')
    df = pd.read_csv(StringIO(response.text), skipfooter=0, parse_dates=[0], index_col=0, engine='python')

    print(df.tail(1))
    #df = pd.read_csv('MCD.US.csv')

    #Se crean los data frames de cada periodo de tiempo solicitado
    df7d = crearDF( df.tail(7),"7d")

    df52s = crearDF(df.tail(260),"52s")
    
    df5a = crearDF(df.tail(1300),"5a")

    frames = [df7d, df52s, df5a]
    dfTriple = pd.concat(frames)
    #Se envian a google sheets
    guardarTabla(dfTriple,'ELT-Getxerpa',worksheet_name=ticker )

    df.reset_index(inplace=True)
    #Se crea el data frame anual
    dfAnual = crearDFAnual(df,"Anual")
    dfAnual.reset_index(inplace=True)
    print(dfAnual.tail(1))
    #Se envia a google sheets
    guardarTabla(dfAnual,'ELT-Getxerpa',worksheet_name=ticker + " Anual" )
def test():
    df = pd.read_csv('MCD.US.csv')
    #Se crean los data frames de cada periodo de tiempo solicitado
    df7d = crearDF( df.tail(7),"7d")
    df52s = crearDF(df.tail(260),"52s")
    df5a = crearDF(df.tail(1300),"5a")

    frames = [df7d, df52s, df5a]
    dfTriple = pd.concat(frames)
    #Se envian a google sheets
    guardarTabla(dfTriple,'ELT-Getxerpa',worksheet_name='MCD' )

    df.reset_index(inplace=True)
    #Se crea el data frame anual
    dfAnual = crearDFAnual(df,"Anual")
    dfAnual.reset_index(inplace=True)
    print(dfAnual.tail(1))
    #Se envia a google sheets
    guardarTabla(dfAnual,'ELT-Getxerpa',worksheet_name='MCD' + " Anual" )

def bucle():
    os.system("color FF")
    while True:
        print('Indicame que hacer [enter]=B')
        print('B - Bajar datos')
        print('T - Test Local con MCD.US.csv')
        print('S - Salir')
        opcion=input()
        opcion=opcion.upper()
        if opcion=="B":
            bajardatos()
        elif opcion=="T":
            test()
        elif opcion=="S":
            break
        else:
            bajardatos()

if __name__ == '__main__':
    bucle()