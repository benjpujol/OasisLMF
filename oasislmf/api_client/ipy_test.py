get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

from connector_api import connector
from client_v2 import API_models, API_portfolios, API_analyses 

api = connector('http://10.10.0.182:8000/', 'sam', 'password')
models = API_models(api, 'V1/models/')
portfolios = API_portfolios(api, 'V1/portfolios/')
analyses   = API_analyses(api,'V1/analyses/')
