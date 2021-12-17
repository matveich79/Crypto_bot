import requests
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import mysql.connector
import Connect_API as capi

#GLOBAL VARIABLES--------------------------------------------------------------------------------------------------------------------------

#crypDB = mysql.connector.connect(host=PI.myHost, user=PI.myUser, password=PI.myPassword, database=PI.myDatabase)

#FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------

def CompareGraphs(dfMain, secondDF, rowRange, variance):
#This function takes certain amount of rows from one cryptocurrency table of our database and transform de data in

    # dfMain = database you want to "predict"
    # dfSecond = database you want to compare it with
    # rowRange = how many rows (days) you want to compare all together
    # variance = how many times move one position down de data range of the second df

    lowestDifferenceOnSecondDF = {'time':'', 'difference':100.0}

    #This function returns a list with the first sampleDF's date 
    def GetPercentageDifference(dfMain, secondDF, rowRange, variance):
        #This function creates a list with all the values of the open column in percentage:

        def CreatePercentageList(dataFrame, rowRange, variance):
            sampleDF = dataFrame.iloc[variance:rowRange + variance]
            mainColumnOpen = sampleDF['open']
            maxOpenID = mainColumnOpen.idxmax()
            minOpenID = mainColumnOpen.idxmin()

            mainPercentList = []
            for x in range(rowRange):
                percentConversion = (mainColumnOpen[x + variance] - mainColumnOpen[minOpenID])/(mainColumnOpen[maxOpenID] - mainColumnOpen[minOpenID]) * 100
                mainPercentList.append(percentConversion)

            #Add percentage column to data frame:
            #sampleDF['percentage'] = mainPercentList

            return mainPercentList

        listMain = CreatePercentageList(dfMain, rowRange, 0)
        listCompare = CreatePercentageList(secondDF, rowRange, variance)

        differSum = 0.0
        for x in range(rowRange):
            difference = listCompare[x] - listMain[x]
            difference = abs(difference)
            differSum += difference
        averagePercentage = differSum/rowRange
        #print(averagePercentage)
        
        result = [secondDF.at[variance, 'time'], averagePercentage]
        #print('Result:')
        #print(result)
        return result
    
    for x in range(variance + 1):
        time_Difference = GetPercentageDifference(dfMain, secondDF, rowRange, x)

        if time_Difference[1] < lowestDifferenceOnSecondDF['difference']:
            lowestDifferenceOnSecondDF['difference'] = time_Difference[1]
            lowestDifferenceOnSecondDF['time'] = time_Difference[0]
            print(lowestDifferenceOnSecondDF)
    
    print(lowestDifferenceOnSecondDF)
    return lowestDifferenceOnSecondDF



#TESTING---------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    table1 = capi.MySQL_getTable('CARR-USDT')
    table2 = capi.MySQL_getTable('BTC-USDT')
    CompareGraphs(table1, table2, 30, 200)