import os
import google_auth_oauthlib.flow
import googleapiclient.errors
from googleapiclient import discovery
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from typing import List
import base64


class GmailAPI:

    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"


    def __init__(self, oauth_credential_path, gmail_token_path, scopes = ['https://www.googleapis.com/auth/gmail.readonly'],
        api_service_name = 'gmail',
        api_version = 'v1'):
        # From Google Console 
        self.oauth_credential_path = oauth_credential_path
        self.gmail_token_path = gmail_token_path
        self.service = self.fetch_service(scopes, api_service_name, api_version)


    def fetch_service(self, scopes, api_service_name, api_version):
   

        creds = Credentials.from_authorized_user_file(self.gmail_token_path, scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.oauth_credential_path, scopes) #enter your scopes and secrets file
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.gmail_token_path, 'w') as token:
                token.write(creds.to_json())
            
        return googleapiclient.discovery.build(
            api_service_name, api_version, credentials=creds)


    def search_emails(self, query_string: str, label_ids: List=None, userId= 'me'):
        try:
            message_list_response = self.service.users().messages().list(
                userId=userId,
                labelIds=label_ids,
                q=query_string
            ).execute()

            message_items = message_list_response.get('messages')
            next_page_token = message_list_response.get('nextPageToken')
            
            while next_page_token:
                message_list_response = self.service.users().messages().list(
                    userId='me',
                    labelIds=label_ids,
                    q=query_string,
                    pageToken=next_page_token
                ).execute()

                message_items.extend(message_list_response.get('messages'))
                next_page_token = message_list_response.get('nextPageToken')
            return message_items
        except Exception as e:
            raise NoEmailFound('No emails returned')


    def get_file_data(self, message_id, attachment_id, userId= 'me'):
        response = self.service.users().messages().attachments().get(
            userId=userId,
            messageId=message_id,
            id=attachment_id
        ).execute()

        file_data = base64.urlsafe_b64decode(response.get('data').encode('UTF-8'))
        return file_data

    def get_message_detail(self,message_id, msg_format='metadata', metadata_headers: List=None,  userId= 'me'):
        message_detail = self.service.users().messages().get(
            userId=userId,
            id=message_id,
            format=msg_format,
            metadataHeaders=metadata_headers
        ).execute()
        return message_detail


if __name__=='__main__':

    query_string = ''
    save_location = ''

    gmail  = GmailAPI(oauth_credential_path="",
                    gmail_token_path='')
    email_messages = gmail.search_emails(query_string)

    email_message = email_messages[0]

    messageDetail = gmail.get_message_detail(email_message['id'], msg_format='full', metadata_headers=['parts'])
    messageDetailPayload = messageDetail.get('payload')

    if 'parts' in messageDetailPayload:
        for msgPayload in messageDetailPayload['parts']:
            file_name = msgPayload['filename']
            body = msgPayload['body']
            if 'attachmentId' in body:
                attachment_id = body['attachmentId']
                attachment_content = gmail.get_file_data(email_message['id'], attachment_id)

                with open(os.path.join(save_location, file_name), 'wb') as _f:
                    _f.write(attachment_content)
                    print(f'File {file_name} is saved at {save_location}')