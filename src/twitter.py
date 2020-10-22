import json
from requests_oauthlib import OAuth1Session

CK = 'kLTEaiP31d7khvfWhDwmi1n3L'
CS = 'lOLD4IJo9gHN5tcuK7W3gLRJuBTozZtrFTpcUZEPBb0ux6r4GT'
AT = '916686298370400256-nr7V9FLTG5McyPjCdljCIDcWQMfidBx'
ATS = 'FhBHhdDzsAhNMGmqA4WFNI73Bp9doA4bsT5RSF2NHZ4YN'

class Tweet:
  twitter = OAuth1Session(CK, CS, AT, ATS)
  text_endpoint = "https://api.twitter.com/1.1/statuses/update.json"
  media_endpoint = "https://upload.twitter.com/1.1/media/upload.json"
  api_endpoint = "https://test-api.coodori.com/api/v1/cash_flows?cik=1001039"

  def __init__(self, cik):
      self.cik = cik

  def upload_img(self):
      pass

  def tweet(self, msg):
     params = {"status" : msg}
     res = self.twitter.post(Tweet.text_endpoint, params = params)
     if res.status_code == 200:
        print("Success.")
     else:
        print("Failed. : %d"% res.status_code)