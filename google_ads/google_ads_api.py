
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

#start_date = datetime().today()

class GoogleAdsAPI:

    def __init__(self, yaml_credentials_path, customer_id, api_version="v12"):
        self.client = GoogleAdsClient.load_from_storage(path=yaml_credentials_path,version=api_version)
        self.customer_id = customer_id
    
    def fetch_data(self, query, perform_on_result):
        ga_service = self.client.get_service("GoogleAdsService")

        #search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query

        stream = ga_service.search_stream(search_request)

        raw_data = []
        for batch in stream:
            for row in batch.results:
                raw_data.append(perform_on_result(row))
                
        return raw_data






if __name__=='__main__':
    yesterday = '2022-12-12'


    query = f"""
        SELECT
          segments.date,
          campaign.name,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.all_conversions
        FROM campaign
        WHERE segments.date BETWEEN "2022-01-01" AND "{yesterday}"
        ORDER BY segments.date DESC"""

    customer_id = ''
    api = GoogleAdsAPI( yaml_credentials_path='', customer_id=customer_id)


    def treat(row):
        temp = {}
        temp['Day'] = row.segments.date
        temp['CampaignName'] = row.campaign.name
        temp['Media'] = 'GoogleAds'
        temp['Cost'] = row.metrics.cost_micros/1000000
        temp['Impressions'] = row.metrics.impressions
        temp['Clicks'] = row.metrics.clicks
        temp['Conversions'] = row.metrics.all_conversions
        
        return temp

        
    data = api.fetch_data(query=query, perform_on_result=treat)
