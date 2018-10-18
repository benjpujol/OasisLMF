#!/bin/env python

import io
import os
import requests
from requests_toolbelt import MultipartEncoder
from requests import codes as status 
from requests.exceptions import *
from posixpath import join as urljoin
import time
import logging

class OasisAPIClient(object):
    def __init__(self, api_url, api_ver,  logger=None):
        self._logger = logger or logging.getLogger()
        self.tkn_access  = None
        self.tkn_refresh = None

        self.api_url     = api_url
        self.api_vers    = api_ver
        self.api         = requests.Session()
        self.api.headers = {
            'authorization': '',
            'accept': 'application/json',
            'content-type': 'application/json',
        }

    def upload(self, file_path, url, session, content_type='text/csv'):
        with io.open(os.path.abspath(file_path), 'rb') as f:
            m = MultipartEncoder(fields={
                'file': (os.path.basename(file_path), f, content_type)})
            return session.post(url, data=m,
                                headers={'Content-Type': m.content_type})

    def access(self, username, password):
        url  = urljoin(self.api_url,'refresh_token/')
        rsp  = self.api.post(url, json={"username": username,
                                                  "password": password})
        if rsp.status_code == status.ok:
            self.tkn_access  = rsp.json()['access_token']
            self.tkn_refresh = rsp.json()['refresh_token']
            self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_access)
            return True        
        else:
            # Log error & raise execption 
            return rsp.text

    def refresh(self):
        self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_refresh)
        url = urljoin(self.api_url,'access_token/')
        rsp = self.api.post(url)

        if rsp.status_code == status.ok:
            print(rsp.json())
            self.tkn_access  = rsp.json()['access_token']
            self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_access)
            return True
        else:
            # Log error & raise execption 
            return rsp.text




    #@oasis_log
    def health_check(self, poll_attempts=1, retry_delay=5):
        """
        Checks the health of the server.

        :param poll_attempts: The maximum number of checks to make
        :type poll_attempts: int

        :param retry_delay: The amount of time to wait between retry attempts
        :type retry_delay: int

        :return: True If the server is healthy, otherwise False
        """
        url = urljoin(self.api_url, 'helthcheck/')
        rsp = self.api.get(url)

        for attempt in range(poll_attempts):
            try:
                if attempt > 0:
                    rsp = self.api.get(url)
                if rsp.ok:
                    return True
            except RequestException:
                pass

            time.sleep(retry_delay)
        else:
            self._logger.error(
                'Could not connect to the api server after {} attempts. Check it is running and try again later.'.format(
                    poll_attempts,
                )
            )
            return False

    def create_model(self, supplier_id, model_id, version_id, retrys=1):
        try: 
            data = {"supplier_id": supplier_id, "model_id": model_id, "version_id":version_id} 
            url  = urljoin(self.api_url, self.api_vers, 'models/')
            rsp  = self.api.post(url, json=data)
            rsp.raise_for_status()
            return rsp
        except (HTTPError, Timeout, ConnectionError) as e:
            if retrys > 0:
                if rsp.status_code == status.UNAUTHORIZED:
                    self.refresh()
                    self.add_model(supplier, model, version, retrys=retrys-1)
            else:
                # Log error & raise execption 
                pass
        return rsp


    def create_portfolio(self, portfolio_name, src_acc_fp, src_loc_fp, retrys=1):
        try: 
            data = {"name": portfolio_name}
            url  = urljoin(self.api_url, self.api_vers, 'portfolios/')
            rsp  = self.api.post(url, json=data)
            rsp.raise_for_status()
            return rsp
        except (HTTPError, Timeout, ConnectionError) as e:
            if retrys > 0:
                # Refresh token and resend request
                if rsp.status_code == status.UNAUTHORIZED:
                    self.refresh()
                    self.create_analyses()

                # Bad Request or model alreadys exsists
                if rsp.status_code == status.:

            else:
                # Log error & raise execption 
                return rsp
    


'''
    def create_analyses(self, analyses_name, model_id, fp_src_acc, fp_src_loc, retrys=1):
        try: 
            data = {"name": analyses_name, "model": str(model_id)}
            url  = 
            rsp  = 
            rsp.raise_for_status()
            return rsp
         except HTTPError, Timeout, ConnectionError as e:
            if retrys > 0:
                if rsp.status_code == status.UNAUTHORIZED:
                    self.refresh()
                self.create_analyses()
            else:
                # Log error & raise execption 
                return False
'''


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

## Status codes list
https://github.com/requests/requests/blob/master/requests/status_codes.py#L22-L100

'''

