get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

from session_manager import SessionManager
from client import API_models, API_portfolios, API_analyses 

api = SessionManager('http://10.10.0.182:8000/', 'sam', 'password')
models = API_models(api, 'http://10.10.0.182:8000/V1/models/')
portfolios = API_portfolios(api, 'http://10.10.0.182:8000/V1/portfolios/')
analyses   = API_analyses(api,'http://10.10.0.182:8000/V1/analyses/')
