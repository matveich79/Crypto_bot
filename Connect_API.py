#from kucoin.client import Client
#client = Client(api_key, api_secret, api_passphrase)

import requests
import pandas as pd
import time
import openpyxl
from datetime import datetime,timedelta
import mysql.connector

#Connect with mysql server
#print(dir(mysql.connector))
crypDB = mysql.connector.connect(host='localhost', user='root', password='toor', database='crypto_data')

#VARIABLES--------------------------------------------------------------------------------------------------------------------------

#FUNCTIONS----------------------------------------------------------------------------------------------------------------------------

def KC_getHistory(sym = "BTC-USDT"):

    apiUrl = "https://api.kucoin.com"
    candleTime = "1day"
    timeStart = 0
    timeEnd = 0

    #Request data from API:
    parameters = {"symbol":sym, "startAt":timeStart, "endAt":timeEnd, "type":"1day"}
    data = requests.get(f"{apiUrl}/api/v1/market/candles", params = parameters)
    
    #Transform into data frame:
    df = pd.DataFrame(data.json(), columns = ["data"])
    #This gives only one column and all the data on each row into a list format. Print(df) if want to see it.

    #So we create a list of lists with all data:
    listOfLists = []
    for listGet in range(len(df)):
        listOfLists.append(df.at[listGet, 'data'])
    
    #Transform the list of lists into a proper dataFrame:
    columns = ["time","open","close","high","low","volume","turnover"]
    finalDF = pd.DataFrame(listOfLists, columns = columns)

    finalDF.to_excel(f'{sym}_UnixTime.xlsx')

    #Change time from Unix format to date format:
    for row in range(len(finalDF)):
        unixToDate = datetime.fromtimestamp(int(finalDF.xs(row)['time'])).strftime('%Y-%m-%d %H:%M:%S')
        finalDF.xs(row)['time'] = unixToDate
    print(df.at[2, 'data'])
    print(finalDF)
    print(finalDF.at[2, 'time'])

    finalDF.to_excel(f'{sym}.xlsx')
    
    #Create table in MySQL:
    #First change "-" to "_" as MySQL doesnot accept "-" on a table name
    symToSQL = ""
    for letter in sym:
        if letter == "-":
            symToSQL = symToSQL + "_"
        else:
            symToSQL = symToSQL + letter

    myCursor = crypDB.cursor()
    
    #First we try to create a new table in the database and put in all the data:
    try:
        myCursor.execute(
            f"CREATE TABLE {symToSQL} (time DATETIME KEY, open FLOAT(40,20), close FLOAT(40,20), high FLOAT(40,20), low FLOAT(40,20), volume FLOAT(40,20), turnover FLOAT(40,20));"
            )
        print(columns)
        columnsTuple = ("time","open","close","high","low","volume","turnover")
        print(columnsTuple)
        for row in range(len(finalDF)):
            #myCursor.excute(f'INSERT INTO {symToSQL} (time, open, close, high, low, volume, turnover) VALUES ({finalDF.at[row, 'time']}, {finalDF.at[row, 'open']}, {finalDF.at[row, 'close']}, {finalDF.at[row, 'high']}, {finalDF.at[row, 'low']}, {finalDF.at[row, 'volume']}, {finalDF.at[row, 'turnover']},)')
            #val = df.at[row, 'data']

            listRow = df.at[row, 'data']
            listRow[0] = finalDF.at[row, 'time']
            val = tuple(listRow)
            sql = f"INSERT INTO {symToSQL} (time, open, close, high, low, volume, turnover) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            myCursor.execute(sql, val)
            crypDB.commit()

        #myCursor.execute(f"INSERT INTO {symToSQL} VALUES ({val[0]}, {val[1]}, {val[2]}, {val[3]}, {val[4]}, {val[5]}, {val[6]});")

    #print(val)
    #If exception happens it shoul be because there is already a table with that name so we just update it:
    except:
        print('Exception happened')
        #dataFromDB = myCursor.execute(f'SELECT * FROM {symToSQL}')
        #print('This is data from DB:')
        #print(dataFromDB)
        #print('worked?')

    #myCursor.close()
    
    return 'Kucoin history done'


def Coinbase_getdata():
    apiUrl = "https://api.pro.coinbase.com"

    sym = "BTC-USD"

    barSize = "86400" ### 86400 for days
    timeEnd = datetime.now()
    delta = timedelta(minutes = int(barSize)/60) #Minutes = barSize/60
    timeStart = timeEnd - (300*delta)
    timeStart = timeStart.isoformat()
    timeEnd = timeEnd.isoformat()

    parameters = {"start":timeStart, "end":timeEnd, "granularity":barSize}

    data = requests.get(f"{apiUrl}/products/{sym}/candles", params = parameters, headers = {"content-type":"application/xml"})

    df = pd.DataFrame(data.json(), columns = ["time","low","high","open","close","volume"])

    df["date"] = pd.to_datetime(df["time"], unit="s")

    df = df[["date","open","high","low","close","volume"]]
    #df = df.dropna()
    df.to_excel(f'{sym}_data.xlsx')
    #print(data.text)
    #print(df)
    return df

def KC_getMarketsList():
    apiUrl = "https://api.kucoin.com"
    data = requests.get(f"{apiUrl}/api/v1/markets")
    df = pd.DataFrame(data.json(), columns = ["data"])
    print(df)
    return df

def KC_getCurrenciesPairs(market):
    apiUrl = "https://api.kucoin.com"
    parameters = {"market":market}
    data = requests.get(f"{apiUrl}/api/v1/symbols", params = parameters)
    df = pd.DataFrame(data.json())
    print(df)
    return df

def CompareGraphs(graph1, graph2):
    #Aprender como coger y guardar los datos de MYSQL desde python
    #Del grafico 1 seleccionamos el final porque queremos predecir el futuro de ese y hacemos un "for" loop con los segundos.
    #Seleccionar el valor mas alto y el mas bajo en un cierto intervalo de tiempo
    #Porcentuarlos siendo el mas alto 100 y el mas bajo 0 e interpolar el resto de valores: (Valor-Min)/(Max-Min) = valor interpolado
    pass


KC_getHistory('BTC-USDT')
#KC_getMarketsList()
#KC_getCurrenciesPairs('DeFi')
    

