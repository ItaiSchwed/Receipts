from __future__ import print_function

import base64
import os.path
import shutil
import urllib.request
import uuid
from collections import defaultdict
from email.message import EmailMessage
from urllib.error import HTTPError

import numpy as np
import pandas as pd
from PyPDF2 import PdfReader
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from stqdm import stqdm


class MailError(Exception):
    pass


class Receipts:
    LABEL_SENT_ID = 'Label_3932144184306770808'
    LABEL_TO_SEND_ID = 'Label_2086058357651086685'
    SHEET_MAILS_ID = '1P5CM9gbvX-DkNPfX05PmfFTEePwwko_kslXsx1QMsH0'
    SHEET_PAYMENTS_ID = '1bM-fZT5pRDfxTSRj2WseRRH1xuV7QYcqxZLaBJFFqZo'

    def __init__(self, creds):
        self.gmail_service = build('gmail', 'v1', credentials=creds)
        self.sheets_service = build('sheets', 'v4', credentials=creds)
        self.drive_service = build('drive', 'v3', credentials=creds)
        self.refresh()
        self.pdfs_dir = os.path.join('.', 'tmp_pdfs')
        if os.path.exists(self.pdfs_dir):
            shutil.rmtree(self.pdfs_dir)
        os.mkdir(self.pdfs_dir)

    def refresh(self):
        self.mails = self.get_mails()
        self.payments = self.get_payments()

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

    def run(self, text):
        self.refresh()
        results = defaultdict(list)
        exceptions = []
        receipts_folder_id = self.get_receipts_folder_id()
        receipt_urls = [token for token in text.split() if token.startswith('https://mrng.to/')]
        for receipt_url in stqdm(receipt_urls, leave=False):
            file_path = None
            id = None
            date = None
            try:
                id, name, date, amount, file_path = self.extract_data(receipt_url)
                mail_address = self.get_mail_address(name)
                if id not in self.payments.id.values:
                    self.send_mail(mail_address, id, date, amount, file_path)
                    self.save_to_drive(receipts_folder_id, self.get_name(name), file_path, id, date)
                    self.payments = pd.concat([self.payments,
                                               pd.DataFrame({'id': [id], 'name': [name],
                                                             'date': [date], 'amount': [amount]})], axis=0)
                    results['sent'].append(receipt_url)
                else:
                    results['already_sent'].append(receipt_url)
            except MailError as error:
                exceptions.append(f'{receipt_url} - {error.args[0]}')
                if file_path is not None and id is not None and date is not None:
                    self.save_to_drive(receipts_folder_id, "NOT_SENT", file_path, id, date)
        if len(exceptions) != 0:
            self.send_exceptions_mail(exceptions)

        self.write_payments()

        if len(exceptions) != 0:
            results['error'].extend(exceptions)
        return results

    def get_mail_address(self, account_name):
        name_index = np.argwhere(account_name == self.mails.account_name.to_numpy()).reshape(-1)
        if name_index.shape[0] == 0:
            raise MailError(f'{account_name} doesn\'t appear in the google sheet')
        name_index = name_index.item()
        mail_address = self.mails.mail[name_index]
        if mail_address == '':
            raise MailError(f'{self.mails.name[name_index]} doesn\'t have a mail address')
        return mail_address

    def get_receipts_folder_id(self):
        folder_query = "mimeType='application/vnd.google-apps.folder' and name='receipts'"
        folder_results = self.drive_service.files().list(q=folder_query, fields="files(id, name)").execute()

        # Print folder name and ID
        if len(folder_results['files']) > 0:
            return folder_results['files'][0]['id']
        folder_metadata = {'name': 'receipts', 'mimeType': 'application/vnd.google-apps.folder'}
        folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')

    def extract_data(self, receipt_url):
        file_path = os.path.join(self.pdfs_dir, f'{uuid.uuid4()}.pdf')
        try:
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
        except HTTPError:
            raise MailError('url couldn\'t be opened, maybe it expired')

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

    def send_mail(self, mail_address, id, date, total, file_path):
        message = EmailMessage()
        message.set_content(f'''
            שלום רב,
            מצ״ב קבלה מספר {id} על סך {total} אשר הופקה בתאריך {date}

            בברכה,
            קהילת הצעירים של גבעת שמואל.''')

        with open(file_path, 'rb') as content_file:
            content = content_file.read()
            message.add_attachment(content, maintype='application',
                                   subtype=(file_path.split('.')[1]), filename=f'receipt{id}.pdf')
        message['To'] = mail_address
        message['From'] = 'kehilat.haz@gmail.com'
        message['Subject'] = 'קבלה מקהילת הצעירים של גבעת שמואל'
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {'raw': encoded_message}
        try:
            self.gmail_service.users().messages().send(userId="me", body=create_message).execute()
        except HttpError:
            raise MailError(f'{mail_address} does\'nt exists')

    def get_name(self, account_name):
        name_index = np.argwhere(account_name == self.mails.account_name.to_numpy()).reshape(-1)
        if name_index.shape[0] == 0:
            raise MailError(f'{account_name} doesn\'t appear in the google sheet')
        name_index = name_index.item()
        return self.mails.name[name_index]

    def send_exceptions_mail(self, exceptions):
        message = EmailMessage()
        message.set_content('\n'.join(exceptions) + '\n\n#exception')
        message['To'] = 'kehilat.haz@gmail.com'
        message['From'] = 'kehilat.haz@gmail.com'
        message['Subject'] = 'Exceptions'
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {'raw': encoded_message}
        self.gmail_service.users().messages().send(userId="me", body=create_message).execute()

    def write_payments(self):
        sheet = self.sheets_service.spreadsheets()
        sheet.values().update(spreadsheetId=self.SHEET_PAYMENTS_ID,
                              range='A:Z', valueInputOption='USER_ENTERED',
                              body={'values': [self.payments.columns.tolist(),
                                               *self.payments.drop_duplicates('id').values.tolist()]}).execute()
