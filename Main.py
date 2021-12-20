
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

#First program checks if database is updated. If not, it will do automaticaly.
if capi.IsDbUpdated():
    pass
else:
    print('Updating database...')
    capi.KC_UpdateCurrencies()
    print('Database updated.')