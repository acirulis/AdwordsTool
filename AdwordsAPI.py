from googleads import adwords, errors


class AdwordsAPI(object):
    API_VERSION = 'v201710'

    def __init__(self, client):
        self.client = client


    def changeHistory(self, customerId, campaigns, date):
        self.client.SetClientCustomerId(customerId)
        customer_sync_service = self.client.GetService('CustomerSyncService', version=self.API_VERSION)
        selector = {
            'dateTimeRange': {'min': date + ' 000000 Europe/Riga', 'max': date + ' 235959 Europe/Riga'},
            'campaignIds': campaigns
        }
        r = customer_sync_service.get(selector)
        return r

    def repCmpPerf(self, customerId, repDate):
        self.client.SetClientCustomerId(customerId)
        report_downloader = self.client.GetReportDownloader(version=self.API_VERSION)
        # report_query = ('SELECT CampaignId, AdGroupId, Id, Criteria, CriteriaType, '
        #                 'FinalUrls, Impressions, Clicks, Cost '
        #                 'FROM CRITERIA_PERFORMANCE_REPORT '
        #                 'WHERE Status IN [ENABLED, PAUSED] '
        #                 'DURING LAST_7_DAYS')
        report_query = ('SELECT CampaignId, CampaignStatus, AdvertisingChannelType,Impressions, Clicks '
                        'FROM CAMPAIGN_PERFORMANCE_REPORT '
                        'DURING ' + repDate + ',' + repDate)
        # print(report_query)
        return report_downloader.DownloadReportAsStringWithAwql(
            report_query, 'CSV', skip_report_header=True, skip_column_header=True,
            skip_report_summary=True, include_zero_impressions=True)

    def getAllCampaigns(self, customerId):
        self.client.SetClientCustomerId(customerId)
        campaign_service = self.client.GetService('CampaignService', version=self.API_VERSION)
        selector = {
            'fields': ['Id', 'Name']
        }
        ret = campaign_service.get(selector)
        lst = []
        for c in ret.entries:
            lst.append(c.id)
        return lst

    def getCampaignCriteria(self, customerId, criterionId, campaignId):
        self.client.SetClientCustomerId(customerId)
        c_criterion = self.client.GetService('CampaignCriterionService', version=self.API_VERSION)
        selector = {
            'fields': ['Id', 'CriteriaType', 'PlatformName','ChannelName', 'DisplayName','LocationName'],
            'predicates': [
                {
                    'field': 'Id',
                    'operator': 'IN',
                    'values': criterionId
                },
                {
                    'field': 'CampaignId',
                    'operator': 'IN',
                    'values': campaignId
                }
            ]
        }
        ret = c_criterion.get(selector)
        return ret

    def getAdGroupCriteria(self, customerId, criterionId, campaignId):
        self.client.SetClientCustomerId(customerId)
        c_criterion = self.client.GetService('AdGroupCriterionService', version=self.API_VERSION)
        selector = {
            'fields': ['Id', 'CriteriaType', 'ApprovalStatus', 'BidModifier', 'BiddingStrategyName', 'ChannelName', 'CpcBid', 'CpmBid', 'CpmBidSource', 'DestinationUrl', 'DisplayName', 'FirstPageCpc','KeywordText'],
            'predicates': [
                {
                    'field': 'Id',
                    'operator': 'IN',
                    'values': criterionId
                },
                {
                    'field': 'CampaignId',
                    'operator': 'IN',
                    'values': campaignId
                }
            ]
        }
        ret = c_criterion.get(selector)
        return ret

    def getAllAccounts(self):
        accounts = []
        try:
            managed_customer_service = self.client.GetService('ManagedCustomerService', version=self.API_VERSION)
            selector = {
                'fields': ['CustomerId', 'Name', 'AccountLabels'],
            }
            data = managed_customer_service.get(selector)
            for account in data.entries:
                account_name = ''
                if hasattr(account, 'name'):
                    account_name = account.name
                el = {}
                el['account_name'] = account_name
                el['customer_id'] = account.customerId
                accounts.append(el)

        except errors.GoogleAdsError as e:
            print(e)
            return []
        return accounts

    def GetCampaignStatuses(self, customerId):
        self.client.SetClientCustomerId(customerId)
        campaign_service = self.client.GetService('CampaignService', version=self.API_VERSION)
        selector = {
            'fields': ['Id', 'Name', 'Status'],
            # 'dateRange': {'min': '20170101', 'max': '20170101'}
            # 'predicates': [
            #     {
            #         'field': 'Status',
            #         'operator': 'IN',
            #         'values': ['ENABLED'],
            #     }
            # ],
        }
        data = campaign_service.get(selector)
        counter = {'ENABLED': 0, 'PAUSED': 0, 'REMOVED': 0, 'UNKNOWN': 0}
        if hasattr(data, 'entries'):
            for cmp in data.entries:
                counter[cmp.status] += 1
        return counter
