from googleads import adwords
from AdwordsAPI import AdwordsAPI
from DBHelper import DBHelper
from datetime import timedelta, date

CustomerId = 2652326677 #XNET
#CustomerId = 3832046205 #Riepas1

print('Starting...')
AW = AdwordsAPI(adwords.AdWordsClient.LoadFromStorage())
DB = DBHelper()

# Lets empty the table
DB.query('delete from ChangeHistory')
DB.query('delete from AdGroupChangeData')
DB.query('delete from CriteriaData')


print('Getting all campaigns...')
cmpgs = AW.getAllCampaigns(CustomerId)


#print(cmpgs)

# print('Gettting campaign criteria')
# data = AW.getCampaignCriteria(CustomerId, criterionId = ['30001', '30002'], campaignId = [319870811])
# print(data)

# print('Getting change history')
# chData = AW.changeHistory(CustomerId, ['319870811'])


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def translateCriterion(CustomerId, CampaignId, CriterionList, type = 'adgroup'):
    if type == 'adgroup':
        crit_data = AW.getAdGroupCriteria(CustomerId, CriterionList, CampaignId)
    else:
        crit_data = AW.getCampaignCriteria(CustomerId,CriterionList, CampaignId)
    if crit_data.totalNumEntries > 0:
        for crit in crit_data.entries:
            row = {'campaignId': CampaignId, 'criterionId': crit.criterion.id, 'data': crit.__str__()}
            DB.insert('CriteriaData',row)



def parseAdGroupChangeData(data, change_id, campaignId):
    for ag in data:
        adg_row = {'change_id': change_id}
        adg_row['adGroupId'] = ag.adGroupId
        adg_row['adGroupChangeStatus'] = ag.adGroupChangeStatus
        if hasattr(ag,'changedAds'):
            adg_row['changedAds'] = ag.changedAds.__str__()
        if hasattr(ag, 'changedCriteria'):
            adg_row['changedCriteria'] = ag.changedCriteria.__str__()
            translateCriterion(CustomerId,campaignId,ag.changedCriteria)
        if hasattr(ag, 'removedCriteria'):
            adg_row['removedCriteria'] = ag.removedCriteria.__str__()
            translateCriterion(CustomerId, campaignId, ag.removedCriteria)
        if hasattr(ag, 'changedAdGroupBidModifierCriteria'):
            adg_row['changedAdGroupBidModifierCriteria'] = ag.changedAdGroupBidModifierCriteria.__str__()
        if hasattr(ag, 'removedAdGroupBidModifierCriteria'):
            adg_row['removedAdGroupBidModifierCriteria'] = ag.removedAdGroupBidModifierCriteria.__str__()
        DB.insert('AdGroupChangeData', adg_row)
    return True

start_date = date(2017, 10, 9)
end_date = date(2017, 10, 20)
for single_date in daterange(start_date, end_date):
    date_to_process = single_date.strftime("%Y%m%d")
    date_to_db = single_date.strftime("%Y-%m-%d")
    chData = AW.changeHistory(CustomerId, cmpgs, date_to_process)
    for camp in chData.changedCampaigns:
        changed = False
        row = {}
        row['date'] = date_to_db
        row['CustomerId'] = CustomerId
        row['CampaignId'] = camp.campaignId
        row['campaignChangeStatus'] = camp.campaignChangeStatus

        if camp.campaignChangeStatus != 'FIELDS_UNCHANGED':
            changed = True
        if hasattr(camp,'changedAdGroups'): changed = True
        if hasattr(camp, 'addedCampaignCriteria'):
            row['addedCampaignCriteria'] = camp.addedCampaignCriteria.__str__()
            translateCriterion(CustomerId, camp.campaignId, camp.addedCampaignCriteria,'campaign')
            changed = True
        if hasattr(camp, 'removedCampaignCriteria'):
            changed = True
            translateCriterion(CustomerId, camp.campaignId, camp.removedCampaignCriteria,'campaign')
            row['removedCampaignCriteria'] = camp.removedCampaignCriteria.__str__()

        #write changed data to db
        if changed:
            print(camp.campaignId)
            id = DB.insert('ChangeHistory',row)
            if hasattr(camp, 'changedAdGroups'):
                parseAdGroupChangeData(camp.changedAdGroups, id, camp.campaignId)




