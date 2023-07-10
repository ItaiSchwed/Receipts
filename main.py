from __future__ import print_function

import uuid
import shutil
import base64
import os.path
import numpy as np
import pandas as pd
import urllib.request

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from tqdm import tqdm
from PyPDF2 import PdfReader
from Levenshtein import distance
from email.message import EmailMessage
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


class MailError(Exception):
    pass


class Receipts:
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly',
              'https://www.googleapis.com/auth/gmail.send',
              'https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/gmail.modify',
              'https://www.googleapis.com/auth/drive']

    LABEL_SENT_ID = 'Label_3932144184306770808'
    LABEL_TO_SEND_ID = 'Label_2086058357651086685'
    SHEET_MAILS_ID = '1P5CM9gbvX-DkNPfX05PmfFTEePwwko_kslXsx1QMsH0'
    SHEET_PAYMENTS_ID = '1bM-fZT5pRDfxTSRj2WseRRH1xuV7QYcqxZLaBJFFqZo'

    def __init__(self):
        creds = self.get_creds()
        self.gmail_service = build('gmail', 'v1', credentials=creds)
        self.sheets_service = build('sheets', 'v4', credentials=creds)
        self.drive_service = build('drive', 'v3', credentials=creds)
        self.mails = self.get_mails()
        self.payments = self.get_payments()
        self.pdfs_dir = os.path.join('.', 'tmp_pdfs')
        if not os.path.exists(self.pdfs_dir):
            os.mkdir(self.pdfs_dir)

    def extract_data(self, receipt_url):
        file_path = os.path.join(self.pdfs_dir, f'{uuid.uuid4()}.pdf')

        with urllib.request.urlopen(receipt_url) as response:
            with open(file_path, 'wb') as f:
                f.write(response.read())

        reader = PdfReader(file_path)
        page = reader.pages[0]
        text = page.extract_text()
        name = text[text.find('לכבוד') + 6:text.find('עמותת בית הכנסת')].strip()
        date = text[text.find('הופק ב') + 7:text.find('לכבוד') - 2]
        id = text[18:text.find('קבלה על תרומה')].strip()
        amount = [token for token in text.split(' ') if '₪' in token][0][1:]

        return id, name, date, amount, file_path

    def get_creds(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'client_secret_14617518150-9ce6hkqidludi6tnjpegb5dt6alkk87g.apps.googleusercontent.com.json',
                    self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        return creds

    def get_mails(self):
        sheet = self.sheets_service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.SHEET_MAILS_ID,
                                    range='A:Z').execute()
        mails = pd.DataFrame([row if len(row) == 3 else [*row, *([''] * (3 - len(row)))]
                              for row in result['values'][1:]], columns=result['values'][0])
        return mails

    def get_payments(self):
        sheet = self.sheets_service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.SHEET_PAYMENTS_ID,
                                    range='A:Z').execute()
        mails = pd.DataFrame(result['values'][1:], columns=result['values'][0])
        return mails

    def write_payments(self):
        sheet = self.sheets_service.spreadsheets()
        sheet.values().update(spreadsheetId=self.SHEET_PAYMENTS_ID,
                              range='A:Z', valueInputOption='USER_ENTERED',
                              body={'values': [self.payments.columns.tolist(),
                                               *self.payments.drop_duplicates('id').values.tolist()]}).execute()

    def get_mail_address(self, account_name):
        name_index = np.argwhere(account_name == self.mails.account_name.to_numpy()).reshape(-1)
        if name_index.shape[0] == 0:
            raise MailError(f'{account_name} doesn\'t appear in the google sheet')
        name_index = name_index.item()
        mail_address = self.mails.mail[name_index]
        if mail_address == '':
            raise MailError(f'{self.mails.name[name_index]} doesn\'t have a mail address')
        return mail_address

    def get_name(self, account_name):
        name_index = np.argwhere(account_name == self.mails.account_name.to_numpy()).reshape(-1)
        if name_index.shape[0] == 0:
            raise MailError(f'{account_name} doesn\'t appear in the google sheet')
        name_index = name_index.item()
        return self.mails.name[name_index]


    def get_data_from_mail(self, result):
        data = (result['payload']['body']['data'] if result['payload']['mimeType'] == 'text/plain'
                else result['payload']['parts'][0]['body']['data'])
        return str(base64.urlsafe_b64decode(data), 'utf-8')

    def get_receipt_urls(self, receipt_mail):
        result = self.gmail_service.users().messages().get(userId='me', id=receipt_mail['id'], format='full').execute()
        full_message = self.get_data_from_mail(result)
        receipt_urls = [token for token in full_message.split() if token.startswith('https://mrng.to/')]
        return receipt_urls

    def send_mail(self, mail_address, id, date, total, receipt_url):
        message = EmailMessage()
        message.set_content(f'''
            שלום רב,
            מצ״ב קישור להורדת קבלה מספר {id} על סך {total} אשר הופקה בתאריך {date}:
            {receipt_url}
            
            בברכה,
            קהילת הצעירים של גבעת שמואל.''')
        message['To'] = mail_address
        message['From'] = 'kehilat.haz@gmail.com'
        message['Subject'] = 'קבלה מקהילת הצעירים של גבעת שמואל'
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {'raw': encoded_message}
        try:
            self.gmail_service.users().messages().send(userId="me", body=create_message).execute()
        except HttpError:
            raise MailError(f'{mail_address} does\'nt exists')

    def get_receipts_folder_id(self):
        folder_query = "mimeType='application/vnd.google-apps.folder' and name='receipts'"
        folder_results = self.drive_service.files().list(q=folder_query, fields="files(id, name)").execute()

        # Print folder name and ID
        if len(folder_results['files']) > 0:
            return folder_results['files'][0]['id']
        folder_metadata = {'name': 'receipts', 'mimeType': 'application/vnd.google-apps.folder'}
        folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')

    def save_to_drive(self, receipts_folder_id, member_name, file_path, file_id, file_date):
        member_folder_query = f"'{receipts_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{member_name}'"
        member_folder_results = self.drive_service.files().list(q=member_folder_query,
                                                                fields="files(id, name)").execute()
        if len(member_folder_results['files']) == 0:
            member_folder_metadata = {
                'name': member_name,
                'parents': [receipts_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            member_folder = self.drive_service.files().create(body=member_folder_metadata, fields='id').execute()
            member_folder_id = member_folder.get('id')
        else:
            member_folder_id = member_folder_results['files'][0]['id']

        file_metadata = {'name': f'{file_id}.{file_date}.pdf', 'parents': [member_folder_id]}
        media = MediaFileUpload(file_path, mimetype='application/pdf')
        file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    def send_exceptions_mail(self, exceptions):
        message = EmailMessage()
        message.set_content('\n'.join(exceptions) + '\n\n#exception')
        message['To'] = 'kehilat.haz@gmail.com'
        message['From'] = 'kehilat.haz@gmail.com'
        message['Subject'] = 'Exceptions'
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {'raw': encoded_message}
        self.gmail_service.users().messages().send(userId="me", body=create_message).execute()

    # def match_names(self):
    #     receipts = pd.read_csv('./receipts.csv').receipts
    #     account_names = np.unique([self.extract_data(receipt_url)[0] for receipt_url in tqdm(receipts)])
    #     pd.Series(account_names).to_csv('account_names.csv')
    #     names = np.array([self.mails.name[np.argmax([np.intersect1d(account_name.split(' '), name.split(' ')).shape[0]
    #                                                  for name in self.mails.name])] for account_name in account_names])
    #     mails_mask = [np.argwhere(name == names).reshape(-1) for name in self.mails.name]
    #     mails_mask = [id.item() if id.shape[0] == 1 else -1 for id in mails_mask]
    #     self.mails['account_name'] = np.array([*account_names, None])[mails_mask]
    #     pd.DataFrame({'account_name': account_names, 'name': names}).to_csv('names_matching.csv')
    #     sheet = self.sheets_service.spreadsheets()
    #     sheet.values().update(spreadsheetId=self.SHEET_MAILS_ID,
    #                           range='A:Z', valueInputOption='USER_ENTERED',
    #                           body={'values': [self.mails.columns.tolist(),
    #                                            *self.mails.drop_duplicates().values.tolist()]}).execute()

    def run(self):
        exceptions = []
        receipt_mails = self.gmail_service.users().messages().list(
            userId="me", q="label:for_automation/to_send").execute()['messages']
        receipts_folder_id = self.get_receipts_folder_id()
        for receipt_mail in tqdm(receipt_mails):
            receipt_urls = self.get_receipt_urls(receipt_mail)
            for receipt_url in tqdm(receipt_urls, leave=False):
                try:
                    id, name, date, amount, file_path = self.extract_data(receipt_url)
                    mail_address = self.get_mail_address(name)
                    if id not in self.payments.id:
                        # self.send_mail(mail_address, id, date, amount, receipt_url)
                        self.save_to_drive(receipts_folder_id, self.get_name(name), file_path, id, date)
                    self.payments = pd.concat(
                        [self.payments, pd.DataFrame({'id': [id], 'name': [name], 'date': [date], 'amount': [amount]})],
                        axis=0)
                except MailError as error:
                    exceptions.append(f'{receipt_url} - {error.args[0]}')
            self.gmail_service.users().messages().modify(
                userId='me', id=receipt_mail['id'], body={'addLabelIds': [self.LABEL_SENT_ID],
                                                          'removeLabelIds': [self.LABEL_TO_SEND_ID]}).execute()
        if len(exceptions) != 0:
            self.send_exceptions_mail(exceptions)

        self.write_payments()
        shutil.rmtree(self.pdfs_dir)


if __name__ == '__main__':
    receipts = Receipts()
    receipts.run()
