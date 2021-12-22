
#Crear otra base de datos solo para spalping con velas de 5 minutos

import numpy as np
import pandas as pd
import time
from datetime import datetime,timedelta,date
import Connect_API as capi



#print(dir(mysql.connector))


#GLOBAL VARIABLES--------------------------------------------------------------------------------------------------------------------------


#FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------

def SymmetricalTriangle(pair,rowRange,variance):
    #This function finds symmetrical triangle pattern in pair.
    timeNow = int(time.time())

    df = capi.KC_ShowHistory(pair,timeNow-25000,timeNow,'5min')
    print(df)
    print(type(df))
    currentHighest = 0.0
    currentLowest = 0.0
    reachedLevel = 0

    for var in range(variance + 1):
        if reachedLevel == 2:
            print(f'Found symmetrical triangle with {pair}')
            return 'done'

        else:
            for x in range(3):
                print(x)
                sampleDF = df.iloc[(x * rowRange) + var : (rowRange + (x * rowRange)) + var]
                #print(sampleDF)
                columnHigh = sampleDF['high']
                columnLow = sampleDF['low']
                #print(columnHigh)
                #print(columnLow)
                print('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')
                print(columnHigh.idxmax())
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
    print('Main is: scalping')

    timeNow = int(time.time())
    print(timeNow)

    ejem = capi.KC_ShowHistory('BTC-USDT',timeNow-25000,timeNow,'5min')
    #print(ejem)
    SymmetricalTriangle('BTC-USDT',20,0)
