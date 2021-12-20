#from mysql.connector.connection_cext import HAVE_CMYSQL
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
#import mysql.connector
import Connect_API as capi


#GLOBAL VARIABLES--------------------------------------------------------------------------------------------------------------------------


#FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------

def CompareGraphs(pairReference, pairCompare, rowRange, variance):
#This function takes certain amount of rows from one pair of the cryptocurrencie table of our database and compares it
#with another pair to find when was less different within certain time (variance)

    # dfMain = database you want to "predict"
    # dfSecond = database you want to compare it with
    # rowRange = how many rows (days) you want to compare all together
    # variance = how many times move one position down de data range of the second df

    dfMain = capi.MySQL_getTable(pairReference)
    secondDF = capi.MySQL_getTable(pairCompare)

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
            #print(lowestDifferenceOnSecondDF)
    
    #print(lowestDifferenceOnSecondDF)
    return lowestDifferenceOnSecondDF


def MultiCompare(target,market,rowRange,variance):
    #Obtener una lista con varias cryptocurrencies con base USDT o BTC.
    #Hacer el for loop que sustituye "-" por "_".
    #Loop de CompareGraphs en varias cryptocurrencies.
    #Guardar resultados de CompareGraphs en una tabla.
    #Devolver fila con menor porcentaje de diferencia.
    targetUnderscore = capi.DashToUnderscore(target)
    listDict = []
    #Create a table:
    #sqlText = 'pair_compared VARCHAR(20) KEY, percentage_difference FLOAT(6,3)'
    #capi.MySQL_CreateTable(targetUnderscore + '_pair_difference', sqlText)

    #Obtein the list of cryptocurrencies:
    pairsList = capi.KC_getCurrenciePairs(market)

    #Compare all the currencies with each other:
    for pair in pairsList:
        valuesList = ()
        pairComp = capi.DashToUnderscore(pair)

        if pairComp == targetUnderscore:
            pass
        else:
            try:
                lowestDiff = CompareGraphs(targetUnderscore,pairComp,rowRange,variance)
                valuesList = (targetUnderscore, pairComp, lowestDiff['time'], lowestDiff['difference'])
                listDict.append(valuesList)
            except:
                pass


    #And then transform the list of lists into a proper dataFrame:
    columns = ["target","compared","time","percentage"]
    dfComp = pd.DataFrame(listDict, columns = columns)
    print(dfComp)

    print(f'Comparing with current value of: {target}')
    #print(dfComp.min())

    print('This are the most similar five: ')
    print(dfComp.nsmallest(5, 'percentage', keep = 'all'))
    
    return 'done'



#TESTING---------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    print('Main is: market analysis')
    #table1 = capi.MySQL_getTable('SOL-USDT')
    #table2 = capi.MySQL_getTable('HTR-USDT')
    #CompareGraphs(table1, table2, 30, 200)
    #cosa = ('hola', 'que pasa', 87.13)
    #col = ('reference', 'compared', 'percentage_difference')
    #capi.MySQL_InsertInTable(cosa, 'compare_graphs', col)
    MultiCompare('CARR-USDT','USDS', 30, 200)

    #CompareGraphs('SOL-USDT', 'HTR-USDT', 30, 200)


