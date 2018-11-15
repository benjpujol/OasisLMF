get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

from session_manager import SessionManager
from client import API_models, API_portfolios, API_analyses, APIClient

#api = SessionManager('http://10.10.0.182:8000/', 'sam', 'password')
#models = API_models(api, 'http://10.10.0.182:8000/V1/models/')
#portfolios = API_portfolios(api, 'http://10.10.0.182:8000/V1/portfolios/')
#analyses   = API_analyses(api,'http://10.10.0.182:8000/V1/analyses/')

api = APIClient(api_url='http://10.10.0.182:8000/',
                api_ver='V1',
                username='sam',
                password='password')


settings = 'test_data/analysis_settings.json'  
acc = 'test_data/SourceAccPiWind.csv'
loc = 'test_data/SourceLocPiWind.csv'

port  = api.upload_inputs(location_fp=loc, accounts_fp=acc)
model = api.models.search({'supplier_id':'OasisIM'}).json()[0]
analysis = api.create_analysis(port['id'], model['id'])

api.run_generate(analysis['id'], poll_interval=0.1)

