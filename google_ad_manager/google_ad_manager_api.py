import tempfile
from datetime import datetime
from datetime import timedelta
from googleads import ad_manager
from googleads import errors
# import pandas as pd
import json
import pathlib

class AdManagerAPI:

    def __init__(self, key_path):
        self.client = ad_manager.AdManagerClient.LoadFromStorage(path=key_path)


    def fetch_report(self, report_job:dict, destiny_directory:str, export_format:str= 'CSV_DUMP',api_version='v202208', ):
        # Initialize a DataDownloader.
        report_downloader = self.client.GetDataDownloader(version=api_version)

        try:
        # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
            # Change to your preferred export format.

            pathlib.Path(destiny_directory).mkdir(parents=True, exist_ok=True)

            report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False,  dir=destiny_directory)

            # Download report data.
            report_downloader.DownloadReportToFile(
                report_job_id, export_format, report_file)

            report_file.close()

            # Display results.
            print('Report job with id "%s" downloaded to:\n%s' % (report_job_id, report_file.name))

            # Return file_name
            return report_file.name

        except errors.AdManagerReportError as e:
            print('Failed to generate report. Error was: %s' % e)

        

if __name__ == '__main__':
  # Initialize client object.
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=3*360)

        # Create report job.
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE','AD_UNIT_NAME'],
            'adUnitView': 'HIERARCHICAL',
            'columns': ['TOTAL_AD_REQUESTS',
                        'TOTAL_RESPONSES_SERVED',
                        'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
                        'TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS',
                        'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date,
            'endDate': end_date
        }
    }
    ad_manager = AdManagerAPI(key_path="XXXXX.yaml") 

    file_path = ad_manager.fetch_report(report_job=report_job,destiny_directory="./result")