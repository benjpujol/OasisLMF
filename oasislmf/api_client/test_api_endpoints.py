#!/bin/env python
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

import io
import os
import requests
from requests_toolbelt import MultipartEncoder
from posixpath import join as urljoin
#import unittest
#import base64

API_BASE_URL  = 'http://10.10.0.182'
API_BASE_PORT = '8000'
API_URL = urljoin(API_BASE_URL,API_BASE_PORT)
API_VER = 'v1'

client_user  = "sam"
client_pass  = "password"
tkn_access = None
tkn_refresh = None

api = requests.Session()
#api.auth = requests.auth.HTTPBasicAuth(client_user, client_pass)
api.headers = {
    'authorization': '',
    'accept': 'application/json',
    'content-type': 'application/json',
}


## Health check
healthcheck_url = urljoin(API_URL, 'helthcheck/')
healthcheck_rsp = api.get(healthcheck_url)



## Get Token
get_token_data = {
    "username": client_user, 
    "password": client_pass
}
get_token_url  = urljoin(API_URL,'refresh_token/')
get_token_rsp  = api.post(get_token_url, json=get_token_data)

if get_token_rsp.status_code == requests.codes.ok:
    tkn_refresh = get_token_rsp.json()['refresh_token']
    tkn_access  = get_token_rsp.json()['access_token']


## Update Token
api.headers['authorization'] = 'Bearer {}'.format(tkn_refresh)
update_token_url = urljoin(API_URL,'access_token/')
update_token_rsp = api.post(update_token_url)
tkn_access = update_token_rsp.json()['access_token']
api.headers['authorization'] = 'Bearer {}'.format(tkn_access)


## Create Model
add_model_data = {
  "supplier_id": "OasisLMF",
  "model_id": "PiWind",
  "version_id": "0.0.0.1"
}
add_model_url = urljoin(API_URL, API_VER, 'models/') 
add_model_rsp = api.post(create_model_url, json=create_model_data)

#if create_model_rsp.status_code == requests.codes.ok:
#    pass
#
#
#elif (r_create_model.status_code == requests.status_codes.codes.bad):
#    print('Model requested in API')
#    r_get_models = api.get(urljoin(API_URL, API_VER, 'models'))




## Create Portfolio
add_portfolio_data = {
  "name": "piwind_test_portfolio"
}
add_portfolio_url = urljoin(API_URL, API_VER, 'portfolios/')
add_portfolio_rsp = api.post(add_portfolio_url, json=add_portfolio_data)
portfolio = add_portfolio_rsp.json()


## Upload ACC / LOC files - https://toolbelt.readthedocs.io/en/latest/uploading-data.html
def upload(file_path, url, session):
    with io.open(f_acc, 'rb') as f:
        m = MultipartEncoder(fields={
            'file': (os.path.basename(file_path), f, 'text/csv')})
        return session.post(url, data=m,
                            headers={'Content-Type': m.content_type})

f_acc = os.path.abspath('test_data/SourceAccPiWind.csv')
f_loc = os.path.abspath('test_data/SourceLocPiWind.csv')
r_upload_acc = upload('test_data/SourceAccPiWind.csv', portfolio['accounts_file'], api)
r_upload_loc = upload('test_data/SourceAccPiWind.csv', portfolio['accounts_file'], api)
#portfolio['reinsurance_info_file']
#portfolio['reinsurance_source_file'] <-- shouldn't this be 'reinsurance_scope_file' ?


gen_analyses_url  = (urljoin(API_URL, API_VER, 'portfolios', portfolio['id'], ' create_analysis')
gen_analyses_data =
gen_analyses_rsp  = api.post(gen_analyses_url,
                             json=analysis_piwind)



## Create an analysis
#analysis_piwind = {
#  "name": "string",
#  "portfolio": str(portfolio['id']),
#  "model": "2",
#}

#r_create_analyses = api.post(API_URL + API_VER + 'analyses/' ,json=analysis_piwind)
#analyses = r_create_analyses.json()


'''
## -------------------------------------------------------------------------- #
Methods to override

class OasisAPIClient(object):

    def __init__(self, oasis_api_url, logger=None):
        pass
    ## Duplicate old func for compatibility
    def upload_inputs_from_directory(
        self, directory, bin_directory=None, do_il=False, do_ri=False, do_build=False, do_clean=False):
        pass
    def run_analysis(self, analysis_settings_json, input_location):
        pass
    def run_analysis(self, analysis_settings_json, input_location):
        pass
    def get_analysis_status(self, analysis_status_location):
        pass
    def run_analysis_and_poll(self, analysis_settings_json, input_location, outputs_directory, analysis_poll_interval=5):
        pass
    def delete_resource(self, path):
        pass
    def delete_exposure(self, input_location):
        pass
    def delete_outputs(self, outputs_location):
        pass
    def download_resource(self, path, localfile):
        pass
    def download_exposure(self, exposure_location, localfile):
        pass
    def download_outputs(self, outputs_location, localfile):
        pass
    def health_check(self, poll_attempts=1, retry_delay=5):
        pass

## Status codes list
https://github.com/requests/requests/blob/master/requests/status_codes.py#L22-L100

'''

