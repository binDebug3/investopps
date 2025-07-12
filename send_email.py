import base64
import os.path
from email.mime.text import MIMEText

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import Tuple



# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

# If this function doesn't work, delete the token.json file and try again
def get_credentials():
    creds = None
    mail_credentials = "meta/cal_credentials.json"
    mail_token = "meta/mail_token.pickle"
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists(mail_token):
        creds = Credentials.from_authorized_user_file(mail_token, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                mail_credentials, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(mail_token, 'w') as token:
            token.write(creds.to_json())

    return creds


def send_email(subject, message_text, recipient):
    """
    Send an email from the user's account.
    :param
        subject: (string) The subject of the email message.
        message_text: (string) The text of the email message.
        recipient: (string) The email address of the recipient.
    :return:
    """
    creds = get_credentials()

    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds, cache_discovery=False)

        # Create the email message
        message = MIMEText(message_text)
        message['to'] = recipient
        message['subject'] = subject

        # Send the email message
        create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        send_message = (service.users().messages().send(userId='me', body=create_message).execute())

        return True, send_message['id']

    except HttpError as error:
        print(f'An error occurred: {error}')
        return False, None
    

def send_table(subject: str, 
               html_content: str,
               recipient: str) -> Tuple[bool, str]:
    """
    Send an HTML email.
    
    Parameters:
    subject: str
        The subject of the email.
    
    html_content: str
        The HTML content of the email.
    
    recipient: str
        The recipient's email address.
    
    Returns:
    success: bool
        True if the email was sent successfully, False otherwise.
        
    message_id: str
        The ID of the sent message if successful, None otherwise.
    """
    creds = get_credentials()

    try:
        service = build('gmail', 'v1', credentials=creds, cache_discovery=False)

        # Set up the HTML email message
        message = MIMEText(html_content, 'html')  # 'html' not 'plain'
        message['to'] = recipient
        message['subject'] = subject

        create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        send_message = service.users().messages().send(userId='me', body=create_message).execute()

        return True, send_message['id']

    except HttpError as error:
        print(f'An error occurred: {error}')
        return False, None
