from asyncore import read
import csv
from email import header
from http import client
from socket import TCP_NODELAY
import sys
import json
from googleapiclient import discovery
from googleapiclient.http import build_http
from httplib2 import Credentials
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow
import requests
import pandas as pd
from spreadsheets import update_spreadsheets, fetch_spreadsheets


class AdMobAPI:
    def __init__(self, key_filepath, app_name, client_id, client_secret):
        scope = 'https://www.googleapis.com/auth/admob.report'
        version = 'v1'
        flow = OAuth2WebServerFlow(client_id=client_id,
                                   client_secret=client_secret,
                                   scope=scope)
        storage = Storage(key_filepath)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage)
        http = credentials.authorize(http=build_http())
        self.admob = discovery.build(app_name, version, http=http)
    # Convert to the list of dictionaries
    
    def accounts(self):
        return self.admob.accounts().list().execute()

        
    def account(self, publisher_id):
        return self.admob.accounts().get(name=self.__accounts_path(publisher_id)).execute()


    def network_report(self, publisher_id, report_spec):
        request = {'reportSpec': report_spec}
        return self.admob.accounts().networkReport().generate(
            parent=self.__accounts_path(publisher_id),
            body=request).execute()


    def mediation_report(self, publisher_id, report_spec):
        request = {'reportSpec': report_spec}
        return self.admob.accounts().mediationReport().generate(
            parent=self.__accounts_path(publisher_id),
            body=request).execute()


    @staticmethod
    def __accounts_path(publisher_id):
        return f"accounts/{publisher_id}"

    @staticmethod
    def report_to_list_of_dictionaries(response):
        result = []
        for report_line in response:
            if report_line.get('row'):
                # print(report_line)
                row = report_line.get('row')
                dm = {}
                if row.get('dimensionValues'):
                    for key, value in row.get('dimensionValues').items():
                        if value.get('value') and value.get('displayLabel'):
                            dm.update({key: value.get('value')})
                            dm.update({key + '_NAME': value.get('displayLabel')})
                        else:
                            dm.update({key: next(filter(None, [value.get('value'), value.get('displayLabel')]))})
                if row.get('metricValues'):
                    try:
                        for key, value in row.get('metricValues').items():
                            dm.update({key: next(filter(None, [value.get('value'), value.get('microsValue'), value.get('integerValue'), value.get('doubleValue')]))})
                    except Exception as err:
                        print(err)
                        
                result.append(dm)
        return result


    def generate_report(self, publisher_id, report_spec):
        


        request = {'reportSpec': report_spec}
        return self.admob.accounts().networkReport().generate(
                parent='accounts/{}'.format(publisher_id),
                body=request).execute()

