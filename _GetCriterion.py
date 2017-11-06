from googleads import adwords
from AdwordsAPI import AdwordsAPI
from DBHelper import DBHelper
from datetime import timedelta, date

CustomerId = 2652326677 #XNET
#CustomerId = 3832046205 #Riepas1

print('Starting...')
AW = AdwordsAPI(adwords.AdWordsClient.LoadFromStorage())
DB = DBHelper()


c = AW.getAdGroupCriteria(CustomerId,[335622252238], 797083126)
print(c)