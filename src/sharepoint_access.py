from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from dotenv import load_dotenv
import os


load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
print(client_id)
print(client_secret)

url = "https://ifrcorg.sharepoint.com/sites/QualityDeliveryUnit221"
file_url = "/sites/QualityDeliveryUnit221/Documents/Quality team files/Project folders/Tracking system working folder/Master_data/EWTS_Master_data_Dummy_Data_070325.xlsx"

ctx = ClientContext(url).with_credentials(ClientCredential(client_id, client_secret))
web = ctx.web
ctx.load(web)
ctx.execute_query()
