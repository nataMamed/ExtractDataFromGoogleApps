from sqlite3 import apilevel
from httplib2 import Credentials
import pandas as pd
from apiclient.discovery import build
from google.oauth2 import service_account
import re


class GoogleSpreadsheetAPI:

    def __init__(self, sheet_id, service_account_credentials, scopes=["https://www.googleapis.com/auth/spreadsheets"]):
        # self.account_credentials = service_account_credentials
        self.sheet_id = sheet_id
        self.service_spreadsheets = self.fetch_spreadsheets(scopes=scopes, credentials=service_account_credentials)
        

    def fetch_spreadsheets(self,  scopes, credentials):

        credentials = service_account.Credentials.from_service_account_info(credentials, 
                                        scopes = scopes) 
                                        
        service = build('sheets', 'v4', credentials=credentials)
        service_spreadsheets = service.spreadsheets()
    

        return service_spreadsheets


    def fetch_spreadsheet_data(self,  sheet_range):

        sheets = self.service_spreadsheets.values().get(spreadsheetId=self.sheet_id, range=sheet_range).execute()
        df_total = pd.DataFrame(sheets['values'][1:], columns=sheets['values'][0])
        
        return df_total


    def update_spreadsheets(self, data, sheet_range, input_mode='USER_ENTERED'):

        self.service_spreadsheets.values().update(
                spreadsheetId=self.sheet_id,
                valueInputOption=input_mode, # USER_ENTERED or RAW
                range=sheet_range,
                body=dict(
                    majorDimension='ROWS',
                    values=data.T.reset_index().T.values.tolist())
            ).execute()

