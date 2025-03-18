from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from dotenv import load_dotenv
import os
import tempfile
import requests

load_dotenv()


def call_creds():
    

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    ctx = ClientContext(site_url).with_credentials(ClientCredential(client_id, client_secret))
    return ctx

def download_batch():
    tenant_id = os.getenv("TENANT")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    TOKEN_URL = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Get access token
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    token_response = requests.post(TOKEN_URL, data=token_data)
    access_token = token_response.json().get("access_token")

    # OneDrive API URL

    # API URL to get Drive ID
    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive"
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get response
    response = requests.get(url, headers=headers)
    data = response.json()

    # Extract Drive ID
    drive_id = data.get("id")
    print(f"Drive ID: {drive_id}")


download_batch()