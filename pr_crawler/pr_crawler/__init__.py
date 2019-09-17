'''
# For atomic transactions.
transaction = db.transaction()
# For batch writes.
batch = db.batch()
'''
import os
from google.oauth2 import service_account
from google.cloud import firestore

# Project ID is determined by the GCLOUD_PROJECT environment variable
_PROJECT = 'supplytracker'
_KEY_FILE = 'supplytracker-3b70d75569c5.json'
_CREDENTIALS = None

cur_path = os.path.realpath(os.path.abspath(__file__))
parent_path = os.path.split(os.path.split(cur_path)[0])[0]
key_file = os.path.join(parent_path, _KEY_FILE)

if os.path.exists(key_file) and os.path.isfile(key_file):
  _CREDENTIALS = service_account.Credentials.from_service_account_file(key_file)
else:
  raise NotImplementedError('service key file not found')

FIRE_DB = firestore.Client(project=_PROJECT, credentials=_CREDENTIALS)

# Parsing urls
_BASE_URI = 'https://www.pr.com'
_START_URI = r'https://www.pr.com/business-directory/'


def encode_url(url):
  url = url.replace('/', 'forward-slash')
  return url


def decode_url(url):
  url = url.replace('forward-slash', '/')
  return url
