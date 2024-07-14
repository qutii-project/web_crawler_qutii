import pandas as pd
import requests
import json
import pandas as pd
# from content_extractor import *
import pandas as pd
# from url_builder import *
from bs4 import BeautifulSoup
import yaml
from datetime import datetime

import pdfplumber
from PyPDF2 import PdfReader
import os
import xml.etree.ElementTree as ET 
import xmltodict
from api_request.send_requests import send_request
from publishers.springer_nature import (process_sringnature)
from publishers.gates_open import (process_gates_open)
from publishers.plos import (process_plos)
from publishers.eu_research import process_open_eu_research

with open('api_env.yaml', 'r') as file:
    api_config = yaml.safe_load(file)

# print(api_config['springernature'])

def wrapper_func():


    # Tester code:
    # publishers=['gatesopen','plos','rsc','eu_open_research','wellcome','springernature','elsevier']
    publishers=['springernature']
    # publishers=['plos','rsc','wellcome','springernature']


    searchkeywords = ['climate','change']


    # Step1: Create url
    '''
    Each API has different URL , check for the publisher name and get URL
    '''
    method='GET'
    pub = 'eu_open_research'


    configs = api_config['eu_open_research']
    process_open_eu_research(pub,method,searchkeywords,configs)

    # print(send_request('GET','sjnfvdjks'))

    # for i in publishers:
    #     print(f"=================FOR: {i}=========================================")
    #     configs = api_config[i]
    #     if i == 'springernature':
    #         process_sringnature(i,method,searchkeywords,configs)
    #     elif i =='gatesopen':
    #         process_gates_open(i,method,searchkeywords,configs)
    #     elif i=='plos':
    #         process_plos(i,method,searchkeywords,configs)
    #     elif i == 'eu_open_research':
    #         process_open_eu_research(i,method,searchkeywords,configs)
    #     elif i =='elsevier':
    #         process_elsevier(i,method,searchkeywords,configs)
            

        
        


wrapper_func()