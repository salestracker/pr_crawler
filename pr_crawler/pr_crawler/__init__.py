'''
# For atomic transactions.
transaction = db.transaction()
# For batch writes.
batch = db.batch()
'''

from google.cloud import firestore

# Project ID is determined by the GCLOUD_PROJECT environment variable
fire_db = firestore.Client()

# Parsing urls
_BASE_URI = 'https://www.pr.com'
_START_URI = r'https://www.pr.com/business-directory/'


def encode_url(url):
  url = url.replace('/', 'forward-slash')
  return url


def decode_url(url):
  url = url.replace('forward-slash', '/')
  return url
