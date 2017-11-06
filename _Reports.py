from googleads import adwords
from AdwordsAPI import AdwordsAPI
import pymysql
import pandas as pd
from io import StringIO
from datetime import timedelta, date

print('Starting...')
AW = AdwordsAPI(adwords.AdWordsClient.LoadFromStorage())



db = pymysql.connect(host="localhost",
                     user="adwords",
                     passwd="z3gOM4v543Ga",
                     db="ad-data",
                     use_unicode=True,
                     charset="utf8")

query = "INSERT INTO CampaignTypeStats(Date, CustomerId, CampaignStatus, AdvertisingChannelType, Count) VALUES(%s, %s, %s, %s, %s)"
# db.query('delete from AdAcc')
cur = db.cursor()

CustomerId = 2652326677 #XNET
CustomerId = 3832046205 #Riepas1
f = {'CampaignId':['count'], 'Impressions':['sum'], 'Clicks': ['sum']}

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2017, 5, 1)
end_date = date(2017, 7, 1)
for single_date in daterange(start_date, end_date):
    date_to_process = single_date.strftime("%Y%m%d")
    date_to_db = single_date.strftime("%Y-%m-%d")
    print('Getting data for ',date_to_process)
    raw = AW.repCmpPerf(CustomerId,date_to_process)
    df = pd.read_csv(StringIO(raw), names=['CampaignId', 'CampaignStatus', 'AdvertisingChannelType', 'Impressions', 'Clicks'])
    stats = df.groupby(['AdvertisingChannelType', 'CampaignStatus']).agg(f)
    # data = stats.add_suffix('_Count').reset_index()
    data = stats.add_suffix('_Count').reset_index()
    print('Inserting into db...')
    for index,row in data.iterrows():
        cur.execute(query, (date_to_db, CustomerId, row['CampaignStatus'],row['AdvertisingChannelType'],row['CampaignId_Count'] ) )
    db.commit()


#db.close()
print('Done!')