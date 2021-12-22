
from mysql.connector.connection_cext import HAVE_CMYSQL
import requests
import numpy as np
import pandas as pd
import time
#import openpyxl
from datetime import datetime,timedelta,date
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
    #This function gets history data from Kucoin API in candle-sticks format and saves it into a database on MySQL. If the db already exists, gets updated.
    #candleTime = 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week

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


    #Change time from Unix format to date format:
    for row in range(len(finalDF)):
        unixToDate = datetime.fromtimestamp(int(finalDF.xs(row)['time'])).strftime('%Y-%m-%d %H:%M:%S')
        finalDF.xs(row)['time'] = unixToDate

    
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
        print(f'Cannot create table {symToSQL}. Probably already exists.')

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
    #This function gets a pair from the databse.

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
    
    #print(f'Finished get table {symToSQL}')
    return df



def KC_getMarketsList():
    #This functions gets a list of the markets offered in kucoin

    apiUrl = "https://api.kucoin.com"
    data = requests.get(f"{apiUrl}/api/v1/markets")
    df = pd.DataFrame(data.json(), columns = ["data"])
    print(df)
    return df



def KC_getCurrenciePairs(market):
    #This function gets all the currencie pairs inside a certain market.

    apiUrl = "https://api.kucoin.com"
    parameters = {"market":market}
    data = requests.get(f"{apiUrl}/api/v1/symbols", params = parameters)
    df = pd.DataFrame(data.json(), columns = ['data'])

    resultList = []
    for x in range(len(df)):
        resultList.append(df.loc[x][0]['symbol'])
    
    #print(resultList)

    return resultList



def KC_UpdateCurrencies():
    #This function gets all the table names in the database and updates updates their records with the function KC_getHistory

    #Create cursor to comunicate with mysql:
    myCursor = crypDB.cursor()

    #Request data from mysql:
    myCursor.execute('SHOW TABLES;')
    tableList = myCursor.fetchall()
    print(tableList)

    for pair in tableList:
        try:
            sym = str(pair)
            symNew = ""
            for letter in sym:
                if letter == "_":
                    symNew = symNew + "-"
                elif letter == "'" or letter == "," or letter == "(" or letter == ")":
                    pass
                else:
                    symNew = symNew + letter
            #print(symNew.upper())
            KC_getHistory(symNew.upper())

        except:
            #This exception is for any other tables created which are not pairs:
            print(f"{pair} not a currencie.")

    return 'done'



def IsDbUpdated():
    #This funcion finds out if the tables of the database are updated or not by checking if the table BTC_USDT is updated (as it is one of the mains ones)
    
    currentDay = str(date.today())

    #Create cursor to comunicate with mysql:
    myCursor = crypDB.cursor()

    #Request data from mysql:
    myCursor.execute(f"SELECT * FROM BTC_USDT WHERE TIME = '" + currentDay + "';")
    result = myCursor.fetchall()

    #Commit to mysql:
    crypDB.commit()

    #Close the cursor (Resets all results, and ensures that the cursor object has no reference to its original connection object).
    myCursor.close()

    if result:
        print('Database is updated')
        return True
    else:
        print('Database needs to update')
        return False



def DashToUnderscore(sym):
    #This function changes "-" to "_" in a string variable
    symNew = ""
    for letter in sym:
        if letter == "-":
            symNew = symNew + "_"
        else:
            symNew = symNew + letter

    return symNew



def MySQL_InsertInTable(item,table,columns):
    #This function saves item in a table.
    #"item" and "columns" must be tuples of the names of the columns.

    colStr = ''
    for col in columns:
        if col == columns[-1]:
            colStr = colStr + col
        else:
            colStr = colStr + col + ', '
    #Create cursor to comunicate with mysql:
    myCursor = crypDB.cursor()

    percent = '%s, ' * (len(columns) - 1) + '%s'
    sql = f"INSERT INTO {table} ({colStr}) VALUES ({percent});"
    print(sql)
    print(item)

    #We use "try" so if row already exists, doesn't give an error and keeps trying with every row in case there is new rows adding to the db:
    try:
        myCursor.execute(sql, item)
    except:
        pass

    #Commit to mysql:
    crypDB.commit()

    #Close the cursor (Resets all results, and ensures that the cursor object has no reference to its original connection object).
    myCursor.close()

    return 'done'



def MySQL_CreateTable(name,columnsSQL):#???????????????????????????????????????
    #Create cursor to comunicate with mysql:
    myCursor = crypDB.cursor()
    
    try:
        myCursor.execute(
            f"CREATE TABLE {name} ({columnsSQL});"
            )
    except:
        pass

    #Commit to mysql:
    crypDB.commit()

    #Close the cursor (Resets all results, and ensures that the cursor object has no reference to its original connection object).
    myCursor.close()
    return 'done'

def KC_getAllMarketHistories(market):
    #This function downloads from Kucoin to our database all the histories of all the pairs from a certain market. It loops the function KC_getHistory to do so.

    marketPairs = KC_getCurrenciePairs(market)
    for pair in marketPairs:
        KC_getHistory(pair)

    return 'done'



def KC_ShowHistory(sym = "BTC-USDT", timeStart = 0, timeEnd = 0, candleTime = "1day"):
    #This function gets history data from Kucoin API in candle-sticks format and saves it into a database on MySQL. If the db already exists, gets updated.
    #candleTime = 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week

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

    #Change time from Unix format to date format:
    for row in range(len(finalDF)):
        unixToDate = datetime.fromtimestamp(int(finalDF.xs(row)['time'])).strftime('%Y-%m-%d %H:%M:%S')
        finalDF.xs(row)['time'] = unixToDate
    
    #print(finalDF)

    return finalDF



#TESTING---------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    print('Main is: connect API')

    #KC_getHistory('HTR-USDT')
    #KC_getMarketsList()
    #KC_UpdateCurrencies()
    #IsDbUpdated()
    #DashToUnderscore('BTC_UD-_-ST')

    #KC_getAllMarketHistories('BTC')

    timeStartDate = datetime(2021, 7, 26, 21, 20) #'2021-07-26 21:20:00'
    #timeEndDate =
    timeStartUnix = time.mktime(timeStartDate.timetuple())
    #timeEndUnix =

    print(timeStartDate)
    print(timeStartUnix)

    timeNow = int(time.time())
    print(timeNow)

    KC_ShowHistory('BTC-USDT',timeNow-25000,timeNow,'5min')