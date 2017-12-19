from googleads import adwords
from AdwordsAPI import AdwordsAPI
import pandas as pd
import numpy as np
from io import StringIO
from datetime import timedelta, date
from DBHelper import DBHelper

AW = AdwordsAPI(adwords.AdWordsClient.LoadFromStorage())
DB = DBHelper()

CustomerId = 2652326677  # XNET
# CustomerId = 7075276416 # Capital
# CustomerId = 2379133775 # Nordea
# CustomerId = 6250334077 # Cv.lv

date = '20171218'

print('Started to get account data')

all_accounts = AW.getAllAccounts()
# print(all_accounts)
print('Got all account data')

total_clicks = 0
total_impressions = 0

for account in all_accounts:
    print('Processing account: ', account['account_name'], account['customer_id'], account['canManageClients'])
    if not account['canManageClients']:
        raw = AW.repAccountPerformance(account['customer_id'], date)
        if raw:
            data = raw.split(',')
            print('Clicks: ', data[1])
            total_clicks += int(data[1])
            print('Impressions: ', data[2])
            total_impressions += int(data[2])

print('Total clicks for yesterday: ', total_clicks)
print('Total impressions for yesterday: ', total_impressions)
