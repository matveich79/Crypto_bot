
import numpy as np
import pandas as pd
import time
from datetime import datetime,timedelta,date

import Connect_API as capi
from Crypto_bot.Connect_API import KC_getMarketsList
import Market_Analysis as ma


#GLOBAL VARIABLES--------------------------------------------------------------------------------------------------------------------------



#FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------

def GetAllMarket(market):
    #This function gets or updates all the history currencies of the selected market.

    pairList = capi.KC_getCurrenciePairs(market)
    for pair in pairList:
        capi.KC_getHistory(pair)
    return 'done'

#MAIN--------------------------------------------------------------------------------------------------------------------------------------

#capi.KC_getMarketsList()
#GetAllMarket('USDS')


#First program checks if database is updated. If not, it will do so automaticaly.
if capi.IsDbUpdated():
    pass
else:
    print('Updating database...')
    capi.KC_UpdateCurrencies()
    print('Database updated.')


#Find patterns:

#Symetrical triangle:
pairList = capi.KC_getCurrenciePairs('USDS')
for pair in pairList:
    try:
        SymmetricalTriangle(pair, 20, 5)
    except:
        pass



#Generate diary inform of best cryptos to buy.
