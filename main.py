import os
import sys

import streamlit as st
from streamlit.runtime.state import SessionState

from receipts import Receipts

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


@st.cache_resource
def get_receipts(creds):
    return Receipts(creds)


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly',
          'https://www.googleapis.com/auth/gmail.send',
          'https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/gmail.modify',
          'https://www.googleapis.com/auth/drive']


def get_creds():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid or creds.expired:
        flow = InstalledAppFlow.from_client_secrets_file(
            'client_secret_14617518150-9ce6hkqidludi6tnjpegb5dt6alkk87g.apps.googleusercontent.com.json', SCOPES)
        creds = flow.run_local_server(port=8097, bind_addr='0.0.0.0')
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def main():
    creds = get_creds()
    if creds:
        receipts = Receipts(creds)
        st.markdown("paste the receipts urls inside:")
        text_cell = st.text_area('', height=200)
        send_button = st.button("Send")

        if send_button:
            results = receipts.run(text_cell)
            cols = st.columns(len(results))
            for i, (type, urls) in enumerate(results.items()):
                cols[i].markdown(type + ':')
                for url in urls:
                    cols[i].markdown("- " + url)


if __name__ == "__main__":
    main()
