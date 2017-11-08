from googleads import adwords
from AdwordsAPI import AdwordsAPI
import pandas as pd
import numpy as np
from io import StringIO
from datetime import timedelta, date
from DBHelper import DBHelper

print('Starting...')
AW = AdwordsAPI(adwords.AdWordsClient.LoadFromStorage())
DB = DBHelper()


DB.query('delete from AdgroupPerformanceReport')

CustomerId = 2652326677  # XNET
# CustomerId = 3832046205 #Riepas1

def convQS(x):
    if x == ' --':
        return -1
    else:
        return int(x)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

f = {'QualityScore': np.mean, 'Impressions':np.sum }

start_date = date(2017, 10, 9)
end_date = date(2017, 10, 10)
for single_date in daterange(start_date, end_date):
    date_to_process = single_date.strftime("%Y%m%d")
    date_to_db = single_date.strftime("%Y-%m-%d")
    print('Getting data for ', date_to_process)
    # First, lets get QS report
    raw = AW.repKeywordPerformance(CustomerId, '20171001')
    df = pd.read_csv(StringIO(raw), names=['CampaignId', 'AdGroupId', 'QualityScore', 'Impressions'])
    df['QualityScore'] = df['QualityScore'].apply(convQS)
    # df.to_pickle('qs_test.pkl') #pd.read_pickle()
    print(df)
    #stats = df.groupby(['CampaignId', 'AdGroupId']).agg(f)
    #data = stats.add_suffix('_Count').reset_index()
    #print(data)
    # Lets get adgroup performance report
    raw = AW.repAdGroupPerformance(CustomerId, date_to_process)
    df = pd.read_csv(StringIO(raw),
        names=['CampaignId', 'AdGroupId', 'BounceRate', 'Ctr', 'Impressions', 'Clicks', 'ConversionRate','RelativeCtr'])
    print('Inserting into db...')
    for index, row in df.iterrows():
        row = {
            'Date': date_to_db,
            'CampaignId': CustomerId,
            'AdGroupId': row['AdGroupId'],
            'BounceRate': row['BounceRate'],
            'Ctr': row['Ctr'],
            'Impressions': row['Impressions'],
            'Clicks': row['Clicks'],
            'ConversionRate': row['ConversionRate'],
            'RelativeCtr': row['RelativeCtr'],
        }
        DB.insert('AdgroupPerformanceReport', row)


print('Done!')
