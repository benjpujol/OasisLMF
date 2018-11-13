#!/bin/env python

import io
import os
import logging
from connector_api import connector


# --- API Endpoint mapping to functions ------------------------------------- #

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
        return '{}{}/{}'.format(
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

    def download(self, ID, file_path):
        return self.connector.download(file_path, self._build_url(ID))

    def delete(self, ID):
        return self.connector.delete(self._build_url(ID))


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

    def create(self, supplier_id, model_id, version_id):
        data = {"supplier_id": supplier_id,
                "model_id": model_id,
                "version_id": version_id}
        return self.connector.post(self.url_endpoint, json=data)

    def update(self, ID, supplier_id, model_id, version_id):
        data = {"supplier_id": supplier_id,
                "model_id": model_id,
                "version_id": version_id}
        return self.connector.put('{}{}/'.format(self.url_endpoint, ID), json=data)

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
        data = {"name": name}  
        return self.connector.post(self.url_endpoint, json=data)

    def update(self, name):
        data = {"name": name}  
        return self.connector.put('{}{}/'.format(self.url_endpoint, ID), json=data)

    def create_analyses(self, ID, name, model_id):
        """ Create new analyses from Exisiting portfolio
        """
        data = {"name": name,
                "model": model_id}
        return self.connector.post('{}{}/create_analysis/'.format(self.url_endpoint, ID), json=data)
                

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
        data = {"name": name,
                "portfolio": portfolio_id,
                "model": model_id } 
        return self.connector.post(self.url_endpoint, json=data)

    def update(self, name, portfolio_id, model_id):
        data = {"name": name,
                "portfolio": portfolio_id,
                "model": model_id } 
        return self.connector.put('{}{}/'.format(self.url_endpoint, ID), json=data)

    def status(self, ID):
        return self.get(ID).json()['status']

    def generate(self, ID):
        return self.connector.post('{}{}/generate_inputs/'.format(self.url_endpoint, ID), json={})

    def generate_cancel(self, ID):
        return self.connector.post('{}{}/cancel_generate_inputs/'.format(self.url_endpoint, ID), json={})

    def run(self, ID):
        return self.connector.post('{}{}/run/'.format(self.url_endpoint, ID), json={})

    def run_cancel(self, ID):
        return self.connector.post('{}{}/cancel/'.format(self.url_endpoint, ID), json={})






# --- API Main Client ------------------------------------------------------- #

class OasisAPIClient(object):
    def __init__(self, api_url, api_ver, timeout=2, logger=None):
        self._logger = logger or logging.getLogger()

        self.api        = connector(api_url, username, password, timeout)
        self.models     = API_models(self.api, '{}/models/'.format(api_ver))
        self.portfolios = API_portfolios(self.api, '{}/portfolios/'.format(api_ver)) 
        self.analyses   = API_analyses(self.api,'{}/analyses/'.format(api_ver))


    def upload_inputs_from_directory(
        self, directory, bin_directory=None, do_il=False, do_ri=False, do_build=False, do_clean=False):
        pass
     
    def run_analysis_and_poll(self, analysis_settings_json, input_location, outputs_directory, analysis_poll_interval=5):
        pass

## -------------------------------------------------------------------------- #
