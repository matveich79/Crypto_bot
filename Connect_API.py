
import requests
import numpy as np
import pandas as pd
import time
import openpyxl
from datetime import datetime,timedelta
import mysql.connector

#To importe from previous directory where there is mysql connection password:
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

import ProtectedInfo as PI


#print(dir(mysql.connector))


#GLOBAL VARIABLES--------------------------------------------------------------------------------------------------------------------------

#Connect with mysql server
crypDB = mysql.connector.connect(host=PI.myHost, user=PI.myUser, password=PI.myPassword, database=PI.myDatabase)



#FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------

def KC_getHistory(sym = "BTC-USDT", timeStart = 0, timeEnd = 0, candleTime = "1day"):
    #This function gets history data from Kucoin API in candle-sticks format and saves it in a database on MySQL. If the db already exists, gets updated.

    apiUrl = "https://api.kucoin.com"
    
    #Request data from API:
    parameters = {"symbol":sym, "startAt":timeStart, "endAt":timeEnd, "type":candleTime}
    data = requests.get(f"{apiUrl}/api/v1/market/candles", params = parameters)
    
    #Transform request into a data frame:
    df = pd.DataFrame(data.json(), columns = ["data"])
    #This gives only one column and all the data on each row into a list format. Print(df) if want to see it.

    #So we create a list of lists with all data:
    listOfLists = []
    for listGet in range(len(df)):
        listOfLists.append(df.at[listGet, 'data'])
    
    #And then transform the list of lists into a proper dataFrame:
    columns = ["time","open","close","high","low","volume","turnover"]
    finalDF = pd.DataFrame(listOfLists, columns = columns)

    #finalDF.to_excel(f'{sym}_UnixTime.xlsx')

    #Change time from Unix format to date format:
    for row in range(len(finalDF)):
        unixToDate = datetime.fromtimestamp(int(finalDF.xs(row)['time'])).strftime('%Y-%m-%d %H:%M:%S')
        finalDF.xs(row)['time'] = unixToDate
    print(finalDF)
    #print(finalDF.at[2, 'time'])

    #finalDF.to_excel(f'{sym}.xlsx')
    
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

    #If exception happens it should be because there is already a table with same name so we just update it:
    except:
        print(f'Probably table {symToSQL} already exists.')

    #Save each row in the db:
    for row in range(len(finalDF)):
        listRow = df.at[row, 'data']
        listRow[0] = finalDF.at[row, 'time']
        val = tuple(listRow)
        sql = f"INSERT INTO {symToSQL} (time, open, close, high, low, volume, turnover) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        #We use "try" so if row already exists, doesn't give an error and keeps trying with every row in case there is new rows adding to the db:
        try:
            myCursor.execute(sql, val)
        except:
            pass

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
    df = pd.DataFrame(data.json(), columns = ['data'])
    print(df)
    print(df.loc[0])
    return df



#TESTING---------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    #table1 = MySQL_getTable('CARR-USDT')
    #table2 = MySQL_getTable('BTC-USDT')

    #KC_getHistory('BTC-USDT')
    #KC_getMarketsList()
    KC_getCurrenciesPairs('DeFi')
    

