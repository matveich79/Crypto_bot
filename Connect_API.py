#from kucoin.client import Client
#client = Client(api_key, api_secret, api_passphrase)

import requests
import numpy as np
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

def KC_getHistory(sym = "BTC-USDT", timeStart = 0, timeEnd = 0, candleTime = "1day"):

    apiUrl = "https://api.kucoin.com"
    
    #Request data from API:
    parameters = {"symbol":sym, "startAt":timeStart, "endAt":timeEnd, "type":candleTime}
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
    print(finalDF)
    #print(finalDF.at[2, 'time'])

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

    #If exception happens it should be because there is already a table with that name so we just update it:
    except:
        print(f'Table {symToSQL} already exists... probably.')

    for row in range(len(finalDF)):
        listRow = df.at[row, 'data']
        listRow[0] = finalDF.at[row, 'time']
        val = tuple(listRow)
        sql = f"INSERT INTO {symToSQL} (time, open, close, high, low, volume, turnover) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        try:
            myCursor.execute(sql, val)
        except:
            pass
    
    #Order rows in sql?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    #myCursor.execute()

    #Commit to mysql:
    crypDB.commit()

    #Close the cursor (Resets all results, and ensures that the cursor object has no reference to its original connection object).
    myCursor.close()
    
    return 'Kucoin history done'



def MySQL_getTable(sym = 'BTC-USDT'):
    #First change "-" to "_" as MySQL doesnot accept "-" on a table name
    symToSQL = ""
    for letter in sym:
        if letter == "-":
            symToSQL = symToSQL + "_"
        else:
            symToSQL = symToSQL + letter

    #Create cursor to comunicate with mysql:
    myCursor = crypDB.cursor()

    #Request data from mysql:
    myCursor.execute(f'SELECT * FROM {symToSQL} ORDER BY time DESC;')
    result = myCursor.fetchall()

    #Close the cursor (Resets all results, and ensures that the cursor object has no reference to its original connection object).
    myCursor.close()

    #Transform result into a data frame:
    columns = ["time","open","close","high","low","volume","turnover"]
    df = pd.DataFrame(result, columns = columns)

    #print(result[0])
    #print(result[0][2])

    #print(df.iloc[:3])
    print(f'Finished get table {symToSQL}')
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



def CompareGraphs(dfMain, secondDF, rowRange):
    #graph1 and 2 have to be in pandas data frame format.
    #Aprender como coger y guardar los datos de MYSQL desde python
    #Del grafico 1 seleccionamos el final porque queremos predecir el futuro de ese y hacemos un "for" loop con los segundos.
    #Seleccionar el valor mas alto y el mas bajo en un cierto intervalo de tiempo
    #Porcentuarlos siendo el mas alto 100 y el mas bajo 0 e interpolar el resto de valores: (Valor-Min)/(Max-Min) = valor interpolado
    
    lowestDifferenceOnSecondDF = {'time':'', 'difference':100.0}

    #This function returns a list with the first sampleDF's date 
    def GetPercentageDifference(dfMain, secondDF, rowRange):
        #This function creates a list with all :
        def CreatePercentageList(dataFrame, rowRange):
            sampleDF = dataFrame.iloc[:rowRange]
            mainColumnOpen = sampleDF['open']
            maxOpenID = mainColumnOpen.idxmax()
            minOpenID = mainColumnOpen.idxmin()

            mainPercentList = []
            for x in range(rowRange):
                percentConversion = (mainColumnOpen[x] - mainColumnOpen[minOpenID])/(mainColumnOpen[maxOpenID] - mainColumnOpen[minOpenID]) * 100
                mainPercentList.append(percentConversion)

            #Add percentage column to data frame:
            #sampleDF['percentage'] = mainPercentList

            return mainPercentList

        listMain = CreatePercentageList(dfMain, rowRange)
        listCompare = CreatePercentageList(secondDF, rowRange)

        differSum = 0.0
        for x in range(rowRange):
            difference = listCompare[x] - listMain[x]
            difference = abs(difference)
            differSum += difference
        averagePercentage = differSum/rowRange
        print(averagePercentage)
        
        result = [secondDF.at[0, 'time'], averagePercentage]
        print('Result:')
        print(result)
        return result
    
    #for x in range(variance):
    time_Difference = GetPercentageDifference(dfMain, secondDF, rowRange)

    if time_Difference[1] < lowestDifferenceOnSecondDF['difference']:
        lowestDifferenceOnSecondDF['difference'] = time_Difference[1]
        print(time_Difference[0])
        lowestDifferenceOnSecondDF['time'] = time_Difference[0]
    print(lowestDifferenceOnSecondDF)
    

    return lowestDifferenceOnSecondDF



table1 = MySQL_getTable('CARR-USDT')
table2 = MySQL_getTable('BTC-USDT')
CompareGraphs(table1, table2, 30)

#KC_getHistory('BTC-USDT')
#KC_getMarketsList()
#KC_getCurrenciesPairs('DeFi')
    





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