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
    def __init__(self, api_url, username, password, timeout=2):
        self.tkn_access  = None
        self.tkn_refresh = None

        self.url_base    = api_url
        self.timeout     = timeout
        self.api         = self._session_retry()
        self.api.headers = {
            'authorization': '',
            'accept': 'application/json',
            'content-type': 'application/json',
        }

        # Check connectivity & authentication
        self.health_check()
        self._get_access_token(username, password) 


    def _session_retry(
        self,
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504, 401),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            status=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def _get_access_token(self, username, password):
        url  = urljoin(self.url_base,'refresh_token/')
        r = self.api.post(url, json={"username": username, "password": password})

        if r.status_code == status.ok:
            self.tkn_access  = r.json()['access_token']
            self.tkn_refresh = r.json()['refresh_token']
            self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_access)
        elif r.status_code == status.UNAUTHORIZED:    
            print('API Login failed:')
            print(r.text)
            #Raise Oasis Error
        else:
            print('API error on fetching access token:')
            print(r.text)
            #Raise Oasis Error
        return r

    def _refresh_token(self):
        self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_refresh)
        url = urljoin(self.url_base,'access_token/')
        r = self.post('access_token/')
        if r.status_code == status.ok:
            self.tkn_access  = r.json()['access_token']
            self.api.headers['authorization'] = 'Bearer {}'.format(self.tkn_access)
        else:
            print('API error when refreshing token:')
            print(r.text)
            #Raise Oasis Error
        return r



    def upload(self, file_path, url_endpoint, content_type='text/csv'):
        abs_fp = os.path.realpath(os.path.expanduser(file_path))
        with io.open(abs_fp, 'rb') as f:
            m = MultipartEncoder(fields={
                'file': (os.path.basename(file_path), f, content_type)})
            return self.api.post(urljoin(self.url_base, url_endpoint), 
                                 data=m,
                                 timeout=self.timeout,
                                 headers={'Content-Type': m.content_type})

    def download(self, file_path, url_endpoint, chuck_size=1024, overrwrite=False):
        abs_fp = os.path.realpath(os.path.expanduser(file_path))
        if os.path.exists(abs_fp) and not overrwrite:
            error_message = 'Local file alreday exists: {}'.format(abs_fp)
            #self._logger.error(error_message)
            #raise OasisException(error_message)
            raise IOError(error_message)

        r = self.get(url_endpoint, stream=True)
        if  r.ok:
            with io.open(abs_fp, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chuck_size):
                    f.write(chunk)
        else:
            print('Download failed')
           # exception_message = 'GET {} failed: {}'.format(response.request.url, response.status_code)
           # self._logger.error(exception_message)
           # raise OasisException(exception_message)
        return r

    #@oasis_log
    def health_check(self):
        """
        Checks the health of the server.

        """
        try: 
            url = urljoin(self.url_base, 'helthcheck/')
            r = self.api.get(url, timeout=self.timeout)
            return True 
        except Exception as e:
            print('Connection check Failed:')
            print(e) 
            #self._logger.error('Healthcheck failed: {}.'.format(str(e)))


    def get(self, url_endpoint, url_base=None,**kwargs):
        try: 
            if not url_base:
                url_base = self.url_base
            r = self.api.get(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            r.raise_for_status()
        except HTTPError as e:
            print(e)
            if e.response.status_code == 401:
                self._refresh_token()
                r = self.api.get(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            else:
                raise
        return r

    def post(self, url_endpoint, url_base=None, **kwargs):
        try: 
            if not url_base:
                url_base = self.url_base
            r = self.api.post(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            r.raise_for_status()
        except HTTPError as e:
            if e.response.status_code == 401:
                self._refresh_token()
                r = self.api.post(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            else:
                raise
        return r

    def delete(self, url_endpoint, url_base=None, **kwargs):
        try: 
            if not url_base:
                url_base = self.url_base
            r = self.api.delete(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            r.raise_for_status()
        except HTTPError as e:
            if e.response.status_code == 401:
                self._refresh_token()
                r = self.api.delete(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            else:
                raise
        return r

    def put(self, url_endpoint, url_base=None, **kwargs):
        try: 
            if not url_base:
                url_base = self.url_base
            r = self.api.put(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            r.raise_for_status()
        except HTTPError as e:
            if e.response.status_code == 401:
                self._refresh_token()
                r = self.api.put(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            else:
                raise
        return r

    def patch(self, url_endpoint, url_base=None, **kwargs):
        try: 
            if not url_base:
                url_base = self.url_base
            r = self.api.patch(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            r.raise_for_status()
        except HTTPError as e:
            if e.response.status_code == 401:
                self._refresh_token()
                r = self.api.patch(urljoin(url_base, url_endpoint), timeout=self.timeout, **kwargs)
            else:
                raise
        return r
