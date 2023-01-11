import numpy as np
import pandas as pd
from apiclient.discovery import build
from google.oauth2 import service_account




class GoogleAnalyticsAPI:
    
    def __init__(self, service_account_credentials:dict):
        self.credentials = service_account_credentials


    @staticmethod
    def format_summary(response):
        try:
            # create row index
            try: 
                row_index_names = response['reports'][0]['columnHeader']['dimensions']
                row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
                row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                            names = np.array(row_index_names))
            except:
                row_index_named = None
            
            # extract column names
            summary_column_names = [item['name'] for item in response['reports'][0]
                                    ['columnHeader']['metricHeader']['metricHeaderEntries']]
        
            # extract table values
            summary_values = [element['metrics'][0]['values'] for element in response['reports'][0]['data']['rows']]
        
            # combine. I used type 'float' because default is object, and as far as I know, all values are numeric
            df = pd.DataFrame(data = np.array(summary_values), 
                            index = row_index_named, 
                            columns = summary_column_names).astype('float')
        
        except:
            df = pd.DataFrame()
            
        return df


    @staticmethod
    def format_pivot(response):
        try:
            # extract table values
            pivot_values = [item['metrics'][0]['pivotValueRegions'][0]['values'] for item in response['reports'][0]
                            ['data']['rows']]
            
            # create column index
            top_header = [item['dimensionValues'] for item in response['reports'][0]
                        ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
            column_metrics = [item['metric']['name'] for item in response['reports'][0]
                            ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
            array = np.concatenate((np.array(top_header),
                                    np.array(column_metrics).reshape((len(column_metrics),1))), 
                                axis = 1)
            column_index = pd.MultiIndex.from_arrays(np.transpose(array))
            
            # create row index
            try:
                row_index_names = response['reports'][0]['columnHeader']['dimensions']
                row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
                row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                            names = np.array(row_index_names))
            except: 
                row_index_named = None
            # combine into a dataframe
            df = pd.DataFrame(data = np.array(pivot_values), 
                            index = row_index_named, 
                            columns = column_index).astype('float')
        except:
            df = pd.DataFrame()
        return df


    def format_report(self, response):
        summary = self.format_summary(response)
        pivot = self.format_pivot(response)
        if pivot.columns.nlevels == 2:
            summary.columns = [['']*len(summary.columns), summary.columns]
        
        return(pd.concat([summary, pivot], axis = 1))


    def fetch_google_analytics_data(self, body:dict, scopes:list):
        credentials = service_account.Credentials.from_service_account_info(self.credentials, 
                                        scopes = scopes)  
        print("Fetching Analytics...")
        service = build('analyticsreporting', 'v4',cache_discovery=False, credentials=credentials)
        response = service.reports().batchGet(body=body).execute()

        try:
            next_page_token = response['reports'][0]['nextPageToken']
        except:
            next_page_token = ''

        return self.format_report(response), next_page_token


    def fetch_all_ga_data_as_df(self, body:dict, scopes:list):
        df_total = pd.DataFrame()

        counter = 1
        last_token = ''
        while True:
            try:
                print(f'Resquest counter: {counter}')
                df_temp, next_page_token = self.fetch_google_analytics_data( body, scopes)
                counter += 1
                last_token = next_page_token
            except Exception as err:

                print("Somenthing went wrong fetching the data...: ", err)
                continue

            df_temp = df_temp.reset_index()
            df_total = pd.concat([df_temp, df_total], axis=0)
            
            if not next_page_token:
                break
            body["reportRequests"][0]["pageToken"] = last_token
            body["reportRequests"][0]["pageSize"] = "1000"

            print(f"Shape: {df_temp.shape}")

        return df_total

    def fetch_all_ga_data_as_json(self, body:dict, scopes:list):
        counter = 1
        last_token = ''
        all_data = []
        while True:
            try:
                print(f'Resquest counter: {counter}')
                temp_data, next_page_token = self.fetch_google_analytics_data( body, scopes)
                counter += 1
                last_token = next_page_token
            except Exception as err:

                print("Somenthing went wrong fetching the data...: ", err)
                continue
            all_data.extend(temp_data)
            
            if not next_page_token:
                break
            body["reportRequests"][0]["pageToken"] = last_token
            body["reportRequests"][0]["pageSize"] = "1000"

        return all_data


if __name__=='__main__':
    credentials = {}
    timestamp = '2022-08-01'
    view_id = ''
    scopes = ['https://www.googleapis.com/auth/analytics.readonly']

    dimensions = [
            'ga:medium', 
            'ga:campaign',
            'ga:pagePathLevel4',
            'ga:eventCategory', 
            'ga:eventLabel'
        ]
    metrics = [
    "ga:totalEvents", 
    "ga:UniqueEvents"
    "ga:users",
    ]
    str_cols = [
        'EventAction',
        "Medium",
        "Campaign",
        "Pagepathlevel4"
    ]

    int_cols = [
    'TotalEvents', 
    "Uniqueevents", 
    'Users'
    ]
    body = {"reportRequests":
    [{
    "viewId": view_id,

    "dateRanges": [{"startDate": f"{timestamp}", "endDate": f"{timestamp}"}],
    "metrics": [{'expression': metric} for metric in metrics],
    'dimensions': [{'name': dimension} for dimension in dimensions],
    "samplingLevel":  "LARGE",
    },
    ]}
    ga_api = GoogleAnalyticsAPI(service_account_credentials=credentials)
    df_total = ga_api.fetch_all_ga_data_as_df(body=body,scopes=scopes)