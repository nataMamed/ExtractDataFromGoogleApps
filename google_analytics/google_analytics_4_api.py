
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    MetricType
)
from math import ceil
from google.oauth2 import service_account
import pandas as pd



class GoogleAnalytics4API:
    def __init__(self, credential_path):
        self.credentials = service_account.Credentials.from_service_account_file(credential_path)

    @staticmethod
    def fetch_data(response):
        """Create pandas df from results of a runReport call."""

        raw_data = []
        for rowIdx, row in enumerate(response.rows):
            temp_dict = {}
            for i, dimension_value in enumerate(row.dimension_values):
                dimension_name = response.dimension_headers[i].name
                temp_dict[dimension_name]= dimension_value.value

            for i, metric_value in enumerate(row.metric_values):
                metric_name = response.metric_headers[i].name
                temp_dict[metric_name] = metric_value.value
            raw_data.append(temp_dict)
            
        return raw_data
        # [END analyticsdata_print_run_report_response_rows]    



    def fetch_report(self, property_id:str, metrics:list, dimensions:list, start_date:str, end_date:str):
        """Runs a simple report on a Google Analytics 4 property."""

        client = BetaAnalyticsDataClient(credentials=self.credentials)

    
        all_data = []
        offset = 0
        req = 1
        while True:
            request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=100000,
            offset=offset
            )
            response = client.run_report(request)
            tmp = self.fetch_data(response)
            all_data.extend(tmp)
            
            
            requisitions = ceil(response.row_count / 100000)
            # print(requisitions)
            if req == requisitions:
                break
                
            req += 1
            offset += 100000
            
        return all_data
# [END analyticsdata_quickstart]


if __name__=='__main__':
    api = GoogleAnalytics4API('')
    dimensions=[
                Dimension(name="date"),
                Dimension(name="adSourceName"),
                Dimension(name="sessionCampaignName"),
                Dimension(name="sessionMedium"),
                Dimension(name="sessionSource")
            ]
    metrics=[
        Metric(name="publisherAdImpressions"),
        Metric(name="publisherAdClicks"),
        Metric(name="totalRevenue")
        ]
    api.fetch_report(property_id="", metrics=metrics, dimensions=dimensions, start_date="2022-12-01", end_date="yesterday")