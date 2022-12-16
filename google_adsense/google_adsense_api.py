from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
import json
import os

import sys
import google.auth.exceptions
from googleapiclient import discovery


class AdSenseAPI:
    def __init__(self, credentials_json_path, credentials_dat_path, always_require_auth=False, scopes=['https://www.googleapis.com/auth/adsense.readonly']):
        self.json_credentials = credentials_json_path
        self.dat_credentials = credentials_dat_path
        self.aways_require_auth = always_require_auth
        self.scopes = scopes
        self.credentials = self.get_adsense_credentials()
 

    def get_account_id(self, service):

        account_id = None
        response = service.accounts().list().execute()
        if len(response['accounts']) == 1:
            account_id = response['accounts'][0]['name']
        else:
            print('Multiple accounts were found. Please choose:')
            for i, account in enumerate(response['accounts']):
                print(' %d) %s (%s)' % (i + 1, account['displayName'], account['name']))
            selection = (input('Please choose number 1-%d>'
                                % (len(response['accounts']))))
            account_id = response['accounts'][int(selection) - 1]['name']
        return account_id


    def get_adsense_credentials(self, overwrite_existing_credentials=False):
        
        credentials = None
        if (os.path.isfile(self.dat_credentials) and not overwrite_existing_credentials
            and not self.aways_require_auth):
            credentials = Credentials.from_authorized_user_file(self.dat_credentials)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(self.json_credentials, self.scopes)
            credentials = flow.run_local_server()
            with open(self.dat_credentials, 'w') as credentials_file:
                credentials_json = credentials.to_json()
                if isinstance(credentials_json, str):
                    credentials_json = json.loads(credentials_json)
                    json.dump(credentials_json, credentials_file)

        return credentials

    @staticmethod
    def treat_report(response):
        column_names = []
        for col in response['headers']:
            column_names.append(col['name'])
        all_data = []

        for row in response['rows']:
            temp = {}
            for idx, cell in enumerate(row['cells']):        
                temp[column_names[idx]] = cell['value']
            all_data.append(temp)

        return all_data


    def fetch_report(self, start_date, end_date, metrics, dimensions, order_by=['+DATE']):
        with discovery.build('adsense', 'v2', credentials = self.credentials) as service:
            try:
                account_id = self.get_account_id(service)
                
                result = service.accounts().reports().generate(
                    account=account_id, dateRange='CUSTOM',
                    startDate_year=start_date.year, startDate_month=start_date.month, startDate_day=start_date.day,
                    endDate_year=end_date.year,
                    endDate_month=end_date.month, 
                    endDate_day=end_date.day,
                    metrics=metrics,
                    dimensions=dimensions,
                    orderBy=order_by
                    ).execute()
                
                return self.treat_report(response=result)
            except google.auth.exceptions.RefreshError:
                print('The credentials have been revoked or expired, please delete the '
                    '"%s" file and re-run the application to re-authorize.' %
                    self.dat_credentials)

if __name__=="__main__":
    CLIENT_SECRETS_FILE = ''
    CREDENTIALS_FILE = ''
    from datetime import datetime, timedelta

    yesterday  = datetime.now().date() - timedelta(days=1)

    api =  AdSenseAPI(credentials_dat_path=CREDENTIALS_FILE, credentials_json_path=CLIENT_SECRETS_FILE)

    result = api.fetch_report(start_date=yesterday, end_date=yesterday, metrics=['ESTIMATED_EARNINGS', 'IMPRESSIONS',
                         'IMPRESSIONS_RPM', 'ACTIVE_VIEW_VIEWABILITY',
                         'CLICKS'], dimensions=['DATE', 'DOMAIN_NAME'])