#!/bin/env python
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

from session_manager import SessionManager 
api = SessionManager('http://10.10.0.182:8000/', 'sam', 'password')

