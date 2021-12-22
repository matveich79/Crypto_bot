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
    #This function the CompareGraphs function to compare one pair with all the ones inside a certain market.
    #target = is the currencie pair we want to "predict". Example: 'BTC-USDT'
    #market = using capi.KC_getMarketsList() we can obtein a list of markets we can use.
    #rowRange = how many rows/days we want to compare at once every time.
    #variance = til how long in the past want to lo for similarities on the other pair. It is a number of rows/days.

    #First we set the values and change the "-" for "_" which is what our database admits.
    targetUnderscore = capi.DashToUnderscore(target)
    listDict = []

    #Obtein the list of cryptocurrencies:
    pairsList = capi.KC_getCurrenciePairs(market)

    #Compare all the currencies pairs of the market with our pair target:
    for pair in pairsList:
        valuesList = ()
        pairComp = capi.DashToUnderscore(pair)

        #This "if" avoids the target pair to compare with itself.
        if pairComp == targetUnderscore:
            pass
        else:
            try:
                lowestDiff = CompareGraphs(targetUnderscore,pairComp,rowRange,variance)
                valuesList = (targetUnderscore, pairComp, lowestDiff['time'], lowestDiff['difference'])
                listDict.append(valuesList)
            except:
                pass


    #And then transform the listDict into a proper dataFrame:
    columns = ["target","compared","time","percentage"]
    dfComp = pd.DataFrame(listDict, columns = columns)
    #print(dfComp)

    #print(f'Comparing with current value of: {target}')
    #print(dfComp.min())

    print('This are the most similar five: ')
    result = dfComp.nsmallest(5, 'percentage', keep = 'all')

    print(result)
    return result


def SymmetricalTriangle(pair,rowRange,variance):
    #This function finds symmetrical triangle pattern in pair.


    df = capi.MySQL_getTable(pair)
    currentHighest = 0.0
    currentLowest = 0.0
    reachedLevel = 0

    for var in range(variance + 1):
        if reachedLevel == 2:
            print(f'Found symmetrical triangle with {pair}')
            return 'done'

        else:
            for x in range(3):
                sampleDF = df.iloc[(x * rowRange) + var : (rowRange + (x * rowRange)) + var]
                #print(sampleDF)
                columnHigh = sampleDF['high']
                columnLow = sampleDF['low']
                #print(columnHigh)
                #print(columnLow)
                if x == 0:
                    currentHighest = columnHigh.max()
                    currentLowest = columnLow.min()
                    #print(currentHighest)
                    #print(currentLowest)
                else:
                    rangeHighest = columnHigh.max()
                    rangeLowest = columnLow.min()
                    idHighest = columnHigh.idxmax()
                    idLowest = columnLow.idxmin()
                    #print(rangeHighest)
                    #print(rangeLowest)
                    #print(idHighest)
                    #print(idLowest)

                    #The conditions are: the maximmum and minimum value of the next range have to be higher and lower
                    # than the prior values respectively, in at least 10%
                    # and highest value must be always older.
                    if rangeHighest >= currentHighest * 1.1 and rangeLowest <= currentLowest * 0.9 and idHighest > idLowest:
                        currentHighest = rangeHighest
                        currentLowest = rangeLowest
                        reachedLevel = x
                    else:
                        break
    

    if reachedLevel == 2:
        print(f'Found symmetrical triangle with {pair}')

    return


#TESTING---------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    print('Main is: market analysis')
    #table1 = capi.MySQL_getTable('SOL-USDT')
    #table2 = capi.MySQL_getTable('HTR-USDT')
    #CompareGraphs(table1, table2, 30, 200)
    #cosa = ('hola', 'que pasa', 87.13)
    #col = ('reference', 'compared', 'percentage_difference')
    #capi.MySQL_InsertInTable(cosa, 'compare_graphs', col)
    #MultiCompare('CARR-USDT','USDS', 30, 100)

    #CompareGraphs('SOL-USDT', 'HTR-USDT', 30, 200)

    #SymmetricalTriangle('BTC-USDT', 20, 0)
    
    #pairList = capi.KC_getCurrenciePairs('USDS')
    #for pair in pairList:
        #try:
            #SymmetricalTriangle(pair, 20, 5)
        #except:
            #pass
    
    