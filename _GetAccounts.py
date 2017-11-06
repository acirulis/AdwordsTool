from googleads import adwords
from AdwordsAPI import AdwordsAPI
import pymysql

print('Starting...')
AW = AdwordsAPI(adwords.AdWordsClient.LoadFromStorage())

print('Getting all accounts...')
account_list = AW.getAllAccounts()

db = pymysql.connect(host="localhost",
                     user="adwords",
                     passwd="z3gOM4v543Ga",
                     db="ad-data",
                     use_unicode=True,
                     charset="utf8")

query = "INSERT INTO AdAcc(AccName, AccId, CampaignCountActive, CampaignCountPaused, CampaignCountRemoved, CampaignCountUnknown) VALUES(%s, %s, %s, %s, %s, %s)"
db.query('delete from AdAcc')
cur = db.cursor()

for account in account_list:
    cmp_status = AW.GetCampaignStatuses(account['customer_id'])
    cur.execute(query, (str(account['account_name']), account['customer_id'], cmp_status['ENABLED'], cmp_status['PAUSED'], cmp_status['REMOVED'], cmp_status['UNKNOWN']))
    db.commit()
    print('Done for ', account['customer_id'], cmp_status['ENABLED'], cmp_status['PAUSED'])

db.close()
print('Done!')