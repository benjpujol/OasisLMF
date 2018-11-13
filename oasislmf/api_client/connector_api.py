#!/bin/env python

import io
import os

import requests
from requests_toolbelt import MultipartEncoder
from requests import codes as status 
from requests.exceptions import *
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from posixpath import join as urljoin
import time
import logging

class connector(object):
    def __init__(self, api_url, api_ver, username, password, logger=None):
        self._logger = logger or logging.getLogger()
        self.tkn_access  = None
        self.tkn_refresh = None

        self.url_base    = api_url
        self.url_vers    = api_ver
        #self.api         = requests.Session()
        self.api         = self._session_retry()
        self.api.headers = {
            'authorization': '',
            'accept': 'application/json',
            'content-type': 'application/json',
        }
        access_granted = self._get_access_token(username, password) 
        if access_granted is not True:

        except Exception as e:
            print('API Authentication error')
            print(e)


    def _session_retry(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def _get_access_token(self, username, password):
        url  = urljoin(self.url_base,'refresh_token/')
        rsp  = self.api.post(url, json={"username": username, "password": password})
        if rsp.status_code == status.ok:
            self.tkn_access  = rsp.json()['access_token']
            self.tkn_refresh = rsp.json()['refresh_token']
            self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_access)
            return True        
        else:
            # Log error & raise execption 
            return rsp.text

    def _refresh_token(self):
        self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_refresh)
        url = urljoin(self.url_base,'access_token/')
        rsp = self.api.post(url, self.timeout)

        if rsp.status_code == status.ok:
            print(rsp.json())
            self.tkn_access  = rsp.json()['access_token']
            self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_access)
            return True
        else:
            # Log error & raise execption 
            return rsp.text



    def upload(self, file_path, url, content_type='text/csv'):
        with io.open(os.path.abspath(file_path), 'rb') as f:
            m = MultipartEncoder(fields={
                'file': (os.path.basename(file_path), f, content_type)})
            return self.api.post(url, 
                                data=m,
                                timeout=self.timeout,
                                headers={'Content-Type': m.content_type})

    def download(self):
        pass

    #@oasis_log
    def health_check(self, retry_delay=5, retrys=1):
        """
        Checks the health of the server.

        :param poll_attempts: The maximum number of checks to make
        :type poll_attempts: int

        :param retry_delay: The amount of time to wait between retry attempts
        :type retry_delay: int

        :return: True If the server is healthy, otherwise False
        """
        try: 
            url = urljoin(self.url_base, 'helthcheck/')
            rsp = self.api.get(url, timeout=1)
            rsp.raise_for_status()
            return True 
        except Exception as e:
            if retrys > 0:     
                print('Failed - Retry')
                time.sleep(retry_delay)
                self.health_check(retry_delay=retry_delay, retrys=retrys-1)
            
            self._logger.error('Healthcheck failed: {}.'.format(str(e)))
        return False


    def get(self, url, **kwargs):
        try: 
            r = self.api.get(url, **kwargs)
        except Exception as e:
           if r.status_code == status.UNAUTHORIZED:
               self._refresh_token()
        return r

    def post(self, url, **kwargs):
        try: 
            r = self.api.post(url, **kwargs)
        except Exception as e:
           if r.status_code == status.UNAUTHORIZED:
               self._refresh_token()
        return r

    def delete(self, url, **kwargs):
        try: 
            r = self.api.delete(url, **kwargs)
        except Exception as e:
           if r.status_code == status.UNAUTHORIZED:
               self._refresh_token()
        return r

    def put(self, url, **kwargs):
        try: 
            r = self.api.put(url, **kwargs)
        except Exception as e:
           if r.status_code == status.UNAUTHORIZED:
               self._refresh_token()
        return r

    def patch(self, url, **kwargs):
        try: 
            r = self.api.patch(url, **kwargs)
        except Exception as e:
           if r.status_code == status.UNAUTHORIZED:
               self._refresh_token()
        return r

