#!/bin/env python

import io
import os
import logging
from connector_api import connector


class ApiEndpoint(object):
    
    def __init__(self, connector, url_endpoint):
        self.connector = connector
        self.url_endpoint = url_endpoint

    '''
        data: dict of key paris to create

        example:
            new_model = {"supplier_id": 'FooBAR', "model_id": 'UKBF', "version_id":'1'}
            r = models.create(new_model)

            r.json() 
            {
                'id': 8,
                'supplier_id': 'FooBAR',
                'model_id': 'UKBF',
                'version_id': '1',
                'created': '18-11-13T15:48:07.285203+0000',
                'modified': '18-11-13T15:48:07.285616+0000'
            }


    '''
    def create(self, data):
        return self.connector.post(self.url_endpoint, json=data)
    def get(self, ID=None):
        if ID:
            return self.connector.get('{}{}/'.format(self.url_endpoint, ID))
        else:    
            return self.connector.get(self.url_endpoint)
    def delete(self, ID):
        return self.connector.delete('{}{}/'.format(self.url_endpoint, ID))
        

class FileEndpoint(object):
    def __init__(self, connector, url_endpoint, url_resource):
        self.connector = connector
        self.url_endpoint = url_endpoint
        self.url_resource = url_resource

    def _build_url(self, ID):
        return '{}{}{}/{}'.format(
            self.connector.url_base,
            self.url_endpoint,
            ID,
            self.url_resource
        )

    def upload(self, ID, file_path):
        '''
            In [14]: r = portfolios.accounts_file.upload(43, '/home/sam/repos/models/OasisPiWind/tests/data/SourceAccPiWind.csv')
            In [16]: r.text
            Out[16]: '{"created":"18-11-13T16:55:31.290627+0000","file":"/media/a580c956a8414ece95b278d16aa0aa3c.csv"}'
        '''
        return self.connector.upload(file_path, self._build_url(ID))

    def download(self):
        pass
    def delete(self):
        pass


class API_models(ApiEndpoint):
    def search(self, metadata):
        '''
        metadata: dict of key pairs to search for

        example:
            m = {'supplier_id': 'OasisIM'}
            r = models.search(m)
            r.json()

            [{
                'id': 6,
                'supplier_id': 'OasisIM',
                'model_id': 'PiWind',
                'version_id': '1',
                'created': '18-10-17T11:07:33.407742+0000',
                'modified': '18-10-17T11:07:33.408123+0000'
            }]
        '''
        search_string = ''
        for key in metadata:
            search_string += '?{}={}'.format(key, metadata[key])
        return self.connector.get('{}{}'.format(self.url_endpoint, search_string))


class API_portfolios(ApiEndpoint):

    def __init__(self, connector, url_endpoint):
        super().__init__(connector, url_endpoint)
        self.accounts_file = FileEndpoint(self.connector, self.url_endpoint, 'accounts_file/')
        self.location_file = FileEndpoint(self.connector, self.url_endpoint, 'location_file/')
        self.reinsurance_info_file = FileEndpoint(self.connector, self.url_endpoint, 'reinsurance_info_file/')
        self.reinsurance_source_file = FileEndpoint(self.connector, self.url_endpoint, 'reinsurance_source_file/')

    def search(self, metadata):
        search_string = ''
        for key in metadata:
            search_string += '?{}={}'.format(key, metadata[key])
        return self.connector.get('{}{}'.format(self.url_endpoint, search_string))


    def create(self, name):
        """ Create New portfolio
            
            Override ApiEndpoint create method
        """
        data = {
          "name": name,
        }  
        pass
    def update(self, ID, accounts_file=None, location_file=None, ri_info_file=None, ri_source_file=None):
        """ Update Exisiting portfolio
        """
        pass

    def create_analyses(self, ID, model_id):
        """ Create new analyses from Exisiting portfolio
        """
        pass

class API_analyses(ApiEndpoint):

    def __init__(self, connector, url_endpoint):
        super().__init__(connector, url_endpoint)
        self.input_errors_file = FileEndpoint(self.connector, self.url_endpoint, 'input_errors_file/')
        self.input_file = FileEndpoint(self.connector, self.url_endpoint, 'input_file/')
        self.input_generation_traceback_file = FileEndpoint(self.connector, self.url_endpoint, 'input_generation_traceback_file/')
        self.output_file = FileEndpoint(self.connector, self.url_endpoint, 'output_file/')
        self.run_traceback_file = FileEndpoint(self.connector, self.url_endpoint, 'run_traceback_file/')
        self.settings_file = FileEndpoint(self.connector, self.url_endpoint, 'settings_file/')

    def search(self, metadata):
        search_string = ''
        for key in metadata:
            search_string += '?{}={}'.format(key, metadata[key])
        return self.connector.get('{}{}'.format(self.url_endpoint, search_string))
   

    def create(self, name, portfolio_id, model_id):
        data = {
          "name": name,
          "portfolio": portfolio_id,
          "model": model_id
        }
        


    def generate(self, ID):
        pass

    def run(self, ID):
        pass

class OasisAPIClient(object):
    def __init__(self, api_url, api_ver, timeout=5, logger=None):
        self._logger = logger or logging.getLogger()

        self.api        = connector(api_url, username, password)
        self.models     = API_models(self.api, '{}/models/'.format(api_ver))
        self.portfolios = API_portfolios(self.api, '{}/portfolios/'.format(api_ver)) 
        self.analyses   = API_analyses(self.api,'{}/analyses/'.format(api_ver))




    '''
    def create_model(self, supplier_id, model_id, version_id, retrys=1):
        try: 
            data = {"supplier_id": supplier_id, "model_id": model_id, "version_id":version_id} 
            url  = urljoin(self.url_base, self.url_vers, 'models/')
            rsp  = self.api.post(url, json=data)
            rsp.raise_for_status()
            # Req complete  
            return rsp.json()
        except (HTTPError, Timeout, ConnectionError) as e:
            if retrys > 0:
                if rsp.status_code == status.UNAUTHORIZED:
                    self.refresh()
                    self.add_model(supplier, model, version, retrys=retrys-1)
        # req failed
        return rsp


    def create_portfolio(self, portfolio_name, retrys=1):
        try: 
            data = {"name": portfolio_name}
            url  = urljoin(self.url_base, self.url_vers, 'portfolios/')
            rsp  = self.api.post(url, json=data)
            rsp.raise_for_status()
            # Req complete  
            return rsp.json()
        except (HTTPError, Timeout, ConnectionError) as e:
            if retrys > 0:
                # Refresh token and resend request
                if rsp.status_code == status.UNAUTHORIZED:
                    self.refresh()
                    self.create_analyses()

                # Bad Request or model alreadys exsists
                if rsp.status_code == status.BAD:
                    return rsp
        # req failed
        return rsp
   

    def create_analyses(self, analyses_name, model_id, portfolio_id, retrys=1):
        try: 
            data = {"name": analyses_name,
                    "portfolio": model_id,
                    "model": portfolio_id}
            url  = urljoin(self.url_base, self.url_vers, 'analyses/')
            rsp  = self.api.post(url, json=data)
            rsp.raise_for_status()
            # Req complete
            return rsp.json()
        except (HTTPError, Timeout, ConnectionError) as e:
            if retrys > 0:
                # Refresh token and resend request
                if rsp.status_code == status.UNAUTHORIZED:
                    self.refresh()
                    self.create_analyses(analyses_name, model_id, portfolio_id, retrys=retrys-1)
        #req failed
        return rsp


 #   def upload_exposure(self, portfolio_dict, fp_location, fp_account, fp_ri_info=None, fp_ri_scope=None):
 #       try:
 #           

 #       try: 
 #           url  = 
 #           rsp  = 
 #           rsp.raise_for_status()
 #           return rsp
 #       except (HTTPError, Timeout, ConnectionError) as e:
 #           if retrys > 0:
 #               if rsp.status_code == status.UNAUTHORIZED:
 #                   self.refresh()
 #               self.create_analyses()

 #       #req failed
 #       return False

'''        



## -------------------------------------------------------------------------- #
'''
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

