#!/bin/env python
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

from test_api_endpoints import OasisAPIClient
api = OasisAPIClient('http://10.10.0.182:8000/', 'V1')
api.access('sam', 'password')

