''''
Description: This script acts as a central manager of getting requests from Qutii frontend
PArse the request, builds the URL and extracts relevant text data from respective API calls.


Enhancements: ***
1.
2.
3.

'''


import pandas as pd
import requests
import json
import pandas as pd
from content_extractor import *
import pandas as pd
from url_builder import *
from bs4 import BeautifulSoup
import yaml
from datetime import datetime

import pdfplumber
from PyPDF2 import PdfReader
import os
import xml.etree.ElementTree as ET 
import xmltodict

from tiquu_pg import select_query,insert_query

def send_request(input_url,method):
    received=None
    try:
        if method == 'GET':
            received = requests.get(input_url)
        elif method=='POST':
            received= requests.post(input_url)
        print("Status code:", received.status_code)
        # print("Headers:", received.headers)
        # print("Content (text):", received.text)
        # print("Content (bytes):", received.content)
        # print("URL:", received.url)
        # print("Encoding:", received.encoding)
        return received
    except Exception as e:
        print("Error --> ",e,' ',type(e),e.args[0])
        print(received.text)
        error = "ERROR: "+str(e)+' for '+input_url
        return error

#   Convert to json



# Check for one api:
    

# api_url = "http://api.springernature.com/"

# #API Key
# api_key = "523d9701c994f13cd90286c9078f295a"

# # Collections - (1) meta (2) metadata (3) openaccess
# collection = "openaccess"

# # Result Format - pam, jats, json, jsonp
# result_format = "json"
# keywords="climate"
# # max result - 10 results if "p" parameter is not included
# q= "keyword:" + keywords
# p=100 # max is 100

# url = api_url + collection + "/" + result_format + "?api_key=" + api_key + "&q=" + q +"&p=100"
# #url = api_url + collection + "/" + result_format + "?api_key=" + api_key + "&q=" + q
# print(url)

# resp = send_request(url,'GET')
# print(resp)

def api_parameters(input_source):
    pram={}
    if input_source == 'springernature':
        # construct paramters required
        pram['base_url'] = 'http://api.springernature.com/'
        pram['api_key'] ='523d9701c994f13cd90286c9078f295a' #need to remove this and save as environ variable
        pram['collection']='openaccess'
        pram['format']='json'
        pram['p']=10 #max 100
        
    
    elif input_source == 'gatesopen':
        '''
        documentation: https://gatesopenresearch.org/developers
        | Name  | Type    | Mandatory | Information                                             |
        |-------|---------|-----------|---------------------------------------------------------|
        | q     | String  | Y         | Query terms (see the table below)                       |
        | page  | Integer | N         | 1,2,3                                                   |
        |       |         |           | Retrieve a specific page from the results (e.g. return  |
        |       |         |           | page 3); default is page 1                             |
        | rows  | Integer | N         | Number of results to include per page; default/maximum  |
        |       |         |           | is 100                                                  |
        | start | Integer | N         | Start point in page; default is 0                       |
        | wt    | String  | N         | Values: ‘xml’, ‘json’                                   |
        |       |         |           | Type of file retrieved; default is ‘xml’                |
        |-------|---------|-----------|---------------------------------------------------------|
        returns dois in xml
        '''
        pram['base_url']='https://gatesopenresearch.org/extapi/search?'
        pram['wt']='json' #default
        pram['q']='R_FT' #full text search
        pram['solr']='1'
    elif input_source == 'wellcome':
        pram['base_url'] = "https://api.wellcomecollection.org/catalogue/v2/works?"
        pram['query']=''
    elif input_source=='eu_open_research':
        '''
        doc: https://open-research-europe.ec.europa.eu/developers
        user can only make 100 requests per 60 seconds
        has date functionality
        but returns dois in xml
        '''
        pram['base_url']='https://open-research-europe.ec.europa.eu/extapi/search?q='
        pram['q']='R_FT'
        pram['wt']='json'
        pram['solr']='1'
    elif input_source=='rsc':
        '''
        RSC has a different API structure:
        Revisit
        https://developer.rsc.org/docs/demoapis/1/overview
        '''
        pram['rscdummy']='0'
    elif input_source=='plos':
        '''
        doc: https://api.plos.org/
        type: solr
        date functionality?
        '''
        pram['base_url']='http://api.plos.org/search?q='
        pram['wt']='json'
        pram['q']='everything'
        pram['solr']='1'

    return pram

import hashlib

import sqlite3

# def execute_query(iud_operation, schema, journal_name, hash_value):
#     # Connect to your SQLite database
#     conn = sqlite3.connect('tiquu.db')
#     cursor = conn.cursor()
    
#     # Create the SQL query with placeholders
#     query = f"{iud_operation} count(*) FROM {schema} WHERE journal_name = ? AND hash_value = ?"
    
#     # Execute the query with the parameters
#     cursor.execute(query, (kwargs['journal_name'], hash_value))
    
#     # Fetch the results
#     result = cursor.fetchone()
    
#     # Close the connection
#     cursor.close()
#     conn.close()
    
#     return result

def hash_func(input):
    in_bytes = input.encode('utf-8') 
    hash_object = hashlib.sha256(in_bytes)
    # Get the hexadecimal representation of the hash
    password_hash = hash_object.hexdigest()
    
    return password_hash

def db_query(iud_op,**kwargs):
    iud_operation = iud_op

    if iud_operation == 'SELECT':
        db_query = f"{iud_operation} count(*) from {kwargs['schema']} where journal_name = '{kwargs['journal_name']}' AND hash_value = '{kwargs['hash_value']}'"
        print(db_query)
        try:
            # execute_query(db_query,{'journal_name':kwargs['journal_name'],'hash_value':kwargs['hash_value']})
            conn = sqlite3.connect('tiquu.db')
            cursor = conn.cursor()
            cursor.execute(db_query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            print(result)
            return result
        except:
            print('select query failed')
    elif iud_operation == 'INSERT':
        print("insert the record")
        db_query = f"{iud_operation} INTO {kwargs['schema']} (journal_name, hash_value) VALUES ('{kwargs['journal_name']}', '{kwargs['hash_value']}')"
        print(db_query)
        try:
            # execute_query(db_query,{'journal_name':kwargs['journal_name'],'hash_value':kwargs['hash_value']})
            conn = sqlite3.connect('tiquu.db')
            cursor = conn.cursor()
            cursor.execute(db_query)
            conn.commit()
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            print(result)
            return result
        except:
            print('insert query failed')

def check_duplicates(input_hash,journal):
    search_term = input_hash

    # check if the article exists in db
    result = select_query.get_records('SELECT',{'hash_value':search_term,'schema':'Journals','journal_name':journal})

    if result:
        return True
    else:
        try:
            insert_query.insert_record('INSERT',{'hash_value':search_term,'schema':'Journals','journal_name':journal})
        except Exception as e:
            # implement logger ehre
            print('insertion failed',e)
        finally:
            return False


def process_sringnature(pub,method,keywords,configs):

    def extract_single_page(response,**kwargs):
        articles = response["records"]
        data_df = pd.DataFrame(columns=['Title','DOI','Authors','Full_Text_URL','Full_Text_Content'])
        for article in articles:
            title = article.get("title")    
            doi = article.get("doi")
            print('for doi -> ',doi)
            # duplicayc check code (handle this in in adifferent func)

            calc_hash = hash_func(doi+' '+title)
            result = select_query.get_records('SELECT',hash_value=calc_hash,schema='Journals',journal_name=publisher_config['pub_name'])

            print('db_result is ',result)

            if (result==0):
                result = insert_query.insert_record('INSERT',hash_value=calc_hash,schema='Journals',journal_name=publisher_config['pub_name'])
                print('db_result is ',result)
                creators = article.get("creators")
                authors = []
                for author in creators:
                    authors.append(author['creator'])
                try:
                    doitext_url = article.get("url")[0]['value'].split('org/')[1]
                    full_text_url = f"https://link.springer.com/content/pdf/{doitext_url}.pdf"
                    

                    # to extract the full text content from a link
                    text_response = requests.get(full_text_url)
                    print('Got full text for doi -> ',doi)
                    with open(f"sample.pdf", 'wb') as f:
                        f.write(text_response.content)
                    print('sample pdf created for doi -> ',doi)
                    # soup = BeautifulSoup(full_text_response.content, 'html.parser')
                    # full_text_content = soup.find('article').get_text()
                    # with pdfplumber.open('sample.pdf') as pdf:
                    #     text = pdf.extract_text()
                    #     full_text_content=full_text_content+text
                    
                    reader = PdfReader('sample.pdf')
                    full_text_content=''
                    for i in range(len(reader.pages)):
                        full_text_content=full_text_content+reader.pages[i].extract_text()
                    print('sample pdf data extracted for doi -> ',doi)
                    data_df.loc[len(data_df)] = [title,doi,authors,full_text_url, full_text_content]
                except Exception as e:
                    print("Error for doi->",str(e))
            else:
                print("RECORD is present in db")


            
        
            
            # Insert the data into data_df DataFrame
        
        print(data_df.shape)
        data_df.to_csv(f"{os.path.join(os.getcwd(),'csv_holder')}/page{kwargs['pgcounter']}_results_for_{configs['pub_name']}_time_{datetime.now()}.csv")
        
    def recursive_extraction(response):
        counter=0
        while response["nextPage"]:
            print("i = ",counter)
            if counter == 2: break
            extract_single_page(response,pgcounter=counter)
            new_url=f"http://api.springernature.com{response['nextPage']}"
            print("new url for i =",counter,"->>> ",new_url)
            response = send_request(new_url,method).json()
            counter+=1
        print("Done with all pages")
            

    print('processing springernature url')
    # get_params = api_parameters(pub)
    keywords=keywords[0]
    # print(get_params)
    publisher_config = configs

    # Build URL
    # get_url=url_builder(get_params,keywords,publisher=pub)
    url = f"{publisher_config['base_url']+publisher_config['collection']}/{publisher_config['result_format']}?api_key={publisher_config['api_key']}&q=keyword:{keywords}&p={publisher_config['p']}"
    
    print(url)
    response=send_request(url,method).json()
    print(response)

    if response["nextPage"]:
        # There are more records to be fetched
        recursive_extraction(response)
    else:
        extract_single_page(response)
        #just one page results
    articles = response["records"]
    
    # print(data_df)
    return None
        

    
    

    # check for the nextPage

def process_gates_open(pub,method,keywords,configs):

    def extract_single_gates_page(response_json,**kwargs):
        print('here')
        # articles = response["records"]
        data_df = pd.DataFrame(columns=['DOI','Full_Text_url','Full_Text_Content'])
        for doi in response_json['results']['doi']:
            print('doi is ',doi)
            full_text_url = f"https://gatesopenresearch.org/extapi/article/pdf?doi={doi}"

            calc_hash = hash_func(doi)
            print(calc_hash)
            # postgres changes
            # result = select_query.get_records('SELECT',hash_value=calc_hash,schema='Journals',journal_name=publisher_config['pub_name'])
            result = select_query.get_records('SELECT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])

            print('db_result is ',result)

            if (result==0):
                result = insert_query.insert_record('INSERT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])
                print('insert db_result is ',result)
                text_response = requests.get(full_text_url)
                print('Got full text for doi -> ',doi)
        # for article in articles:
        #     title = article.get("title")    
        #     doi = article.get("doi")
        #     print('for doi -> ',doi)
        #     creators = article.get("creators")
        #     authors = []
        #     for author in creators:
        #         authors.append(author['creator'])
        #     try:
        #         doitext_url = article.get("url")[0]['value'].split('org/')[1]
        #         full_text_url = f"https://link.springer.com/content/pdf/{doitext_url}.pdf"
                

        #         # to extract the full text content from a link
        #         text_response = requests.get(full_text_url)
        #         print('Got full text for doi -> ',doi)
        #         with open(f"sample.pdf", 'wb') as f:
        #             f.write(text_response.content)
        #         print('sample pdf created for doi -> ',doi)
        #         # soup = BeautifulSoup(full_text_response.content, 'html.parser')
        #         # full_text_content = soup.find('article').get_text()
        #         # with pdfplumber.open('sample.pdf') as pdf:
        #         #     text = pdf.extract_text()
        #         #     full_text_content=full_text_content+text
                
        #         reader = PdfReader('sample.pdf')
        #         full_text_content=''
        #         for i in range(len(reader.pages)):
        #             full_text_content=full_text_content+reader.pages[i].extract_text()
        #         print('sample pdf data extracted for doi -> ',doi)
        #         data_df.loc[len(data_df)] = [title,doi,authors,full_text_url, full_text_content]
        #     except Exception as e:
        #         print("Error for doi->",str(e))
            
        
            
        #     # Insert the data into data_df DataFrame
                try:
                    with open(f"sample_gates.pdf", 'wb') as f:
                        f.write(text_response.content)
                        print('sample pdf created for doi -> ',doi)
                        # soup = BeautifulSoup(full_text_response.content, 'html.parser')
                        # full_text_content = soup.find('article').get_text()
                        # with pdfplumber.open('sample.pdf') as pdf:
                        #     text = pdf.extract_text()
                        #     full_text_content=full_text_content+text
                        
                        reader = PdfReader('sample_gates.pdf')
                        full_text_content=''
                        for i in range(len(reader.pages)):
                            full_text_content=full_text_content+reader.pages[i].extract_text()
                        print('sample pdf data extracted for doi -> ',doi)
                        data_df.loc[len(data_df)] = [doi,full_text_url, full_text_content]


                except Exception as e:
                    print('Exception',e)
            else:
                print("RECORD is present in db for gates open")
            data_df.to_csv(f"{os.path.join(os.getcwd(),'csv_holder')}/page{kwargs['pgcounter']}_results_for_{configs['pub_name']}_time_{datetime.now()}.csv")

        
    print('processing gatesopen url')
    # get_params = api_parameters(pub)
    keywords=keywords[0]
    # print(get_params)
    publisher_config = configs

    # Build URL
    # get_url=url_builder(get_params,keywords,publisher=pub)
    url = f"{publisher_config['base_url']}q={publisher_config['q']}:'{keywords}'&wt={publisher_config['wt']}"
    
    print(url)
    response=send_request(url,method)
    decoded_response = response.content.decode("utf-8")
    response_json = json.loads(json.dumps(xmltodict.parse(decoded_response)))
    print(response_json)
    total_pages = int(response_json['results']['@totalNumberOfPages'])
    print('total_pages',total_pages)
    if total_pages >1:
        # extract all pages
        extract_single_gates_page(response_json,pgcounter=1)
        for pg in range(2,int(response_json['results']['@totalNumberOfPages'])+1):
            new_url = url+'&page='+str(pg)
            pg_response= send_request(new_url,method)
            decoded_pgresponse = pg_response.content.decode("utf-8")
            pg_response_json = json.loads(json.dumps(xmltodict.parse(decoded_pgresponse)))
            extract_single_gates_page(pg_response_json,pgcounter=pg)
    else:
        extract_single_gates_page(response_json,pgcounter=1)
        # extract one page
    # for i in response_json['results']['doi']:
    #     print(i)

    # tree = ET.parse(responseXml)
    # root = tree.getroot() 
    '''for some reason the json format is not working need to parse the xml here
    '''

def process_plos(pub,method,keywords,configs):

    def extract_single_plos_page(response,**kwargs):
        data_df = pd.DataFrame(columns=['DOI','Full_Text_url','Full_Text_Content'])
        for dois in response['response']['docs']:
            page_doi=dois['id']
            print('doi is ',page_doi)
            calc_hash=hash_func(page_doi)
            print(calc_hash)
            result = select_query.get_records('SELECT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])

            print('db_result is ',result)
            if (result==0):
                result = insert_query.insert_record('INSERT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])
                print('db_result is ',result)
                full_text_url = f"http://journals.plos.org/plosone/article/file?id={page_doi}&type=printable"
                text_response = requests.get(full_text_url)
                print('Got full text for doi -> ',page_doi)

                try:
                #     # Insert the data into data_df DataFrame
                    with open(f"sample_plos.pdf", 'wb') as f:
                        f.write(text_response.content)
                        print('sample pdf created for doi -> ',page_doi)
                        # soup = BeautifulSoup(full_text_response.content, 'html.parser')
                        # full_text_content = soup.find('article').get_text()
                        # with pdfplumber.open('sample.pdf') as pdf:
                        #     text = pdf.extract_text()
                        #     full_text_content=full_text_content+text
                        
                        reader = PdfReader('sample_plos.pdf')
                        full_text_content=''
                        for i in range(len(reader.pages)):
                            full_text_content=full_text_content+reader.pages[i].extract_text()
                        print('sample pdf data extracted for doi -> ',page_doi)
                        data_df.loc[len(data_df)] = [page_doi,full_text_url, full_text_content]
                except Exception as e:
                    print("Error for plos doi")
            else:
                print('reocrd is present in db for plos')
        # print(data_df.shape)
        data_df.to_csv(f"{os.path.join(os.getcwd(),'csv_holder')}/page{kwargs['pgcounter']}_results_for_{configs['pub_name']}_time_{datetime.now()}.csv")
        

    print('processing plos url')
    # get_params = api_parameters(pub)
    keywords=keywords[0]
    # print(get_params)
    publisher_config = configs

    # Build URL
    # get_url=url_builder(get_params,keywords,publisher=pub)
    # url = f"{publisher_config['base_url']}everything;'{keywords}'&wt=json"
    url=f"http://api.plos.org/search?q=everything:'{keywords}'&fl=id&start=1&rows=10"
    
    print(url)
    response=send_request(url,method).json()
    print(response)
    total_results = int(response['response']['numFound'])
    batch_size = 10
    if(total_results<=batch_size):
        extract_single_plos_page(response,pgcounter=1)
    else:
        extract_single_plos_page(response,pgcounter=1)
        start_param=1
        temp_counter=2
        while start_param<=total_results:
            if temp_counter==5: break
            # start_param=offset
            if start_param+batch_size<=total_results:
                end_param = start_param+batch_size
            else:
                end_param = total_results
            new_url=f"http://api.plos.org/search?q=everything:'{keywords}'&fl=id&start={start_param}&rows={end_param}"
            response_new=send_request(url,method).json()
            extract_single_plos_page(response_new,pgcounter=temp_counter)

            start_param=end_param
            temp_counter+=1

def process_open_eu_research(pub,method,keywords,configs):


    def extract_single_eu_page(response,**kwargs):
        print('here')
        # articles = response["records"]
        data_df = pd.DataFrame(columns=['DOI','Full_Text_url','Full_Text_Content'])

        for doi in response_json['results']['doi']:
            print('doi is ',doi)
            calc_hash = hash_func(doi)
            print(calc_hash)
            result = select_query.get_records('SELECT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])

            print('db_result is ',result)
            if (result==0):
                result = insert_query.insert_record('INSERT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])
                print('db_result is ',result)
                try:
                    full_text_url = f"https://open-research-europe.ec.europa.eu/extapi/article/pdf?doi={doi}"
                    text_response = requests.get(full_text_url)
                    print('Got full text for doi -> ',doi)
                    
                #     # Insert the data into data_df DataFrame
                    with open(f"sample_eu.pdf", 'wb') as f:
                        f.write(text_response.content)
                        print('sample pdf created for doi -> ',doi)
                        # soup = BeautifulSoup(full_text_response.content, 'html.parser')
                        # full_text_content = soup.find('article').get_text()
                        # with pdfplumber.open('sample.pdf') as pdf:
                        #     text = pdf.extract_text()
                        #     full_text_content=full_text_content+text
                        
                        reader = PdfReader('sample_eu.pdf')
                        full_text_content=''
                        for i in range(len(reader.pages)):
                            full_text_content=full_text_content+reader.pages[i].extract_text()
                        print('sample pdf data extracted for doi -> ',doi)
                        data_df.loc[len(data_df)] = [doi,full_text_url, full_text_content]
                except Exception as e:
                    print("error for doi")
            else:
                print('record present in db')
        # print(data_df.shape)
        data_df.to_csv(f"{os.path.join(os.getcwd(),'csv_holder')}/page{kwargs['pgcounter']}_results_for_{configs['pub_name']}_time_{datetime.now()}.csv")
        
    print('processing open_eu_research url')
    # get_params = api_parameters(pub)
    keywords=keywords[0]
    # print(get_params)
    publisher_config = configs

    # Build URL
    # get_url=url_builder(get_params,keywords,publisher=pub)
    # url = f"{publisher_config['base_url']}everything;'{keywords}'&wt=json"
    url=f"{publisher_config['base_url']}{publisher_config['q']}:'{keywords}'"
    
    print(url)
    response=send_request(url,method)
    decoded_response = response.content.decode("utf-8")
    response_json = json.loads(json.dumps(xmltodict.parse(decoded_response)))
    # print(response_json)
    total_pages = int(response_json['results']['@totalNumberOfPages'])
    print('total_pages',total_pages)
    if total_pages >1:
        # extract all pages
        extract_single_eu_page(response_json,pgcounter=1)
        for pg in range(2,int(response_json['results']['@totalNumberOfPages'])+1):
            new_url = url+'&page='+str(pg)
            pg_response= send_request(new_url,method)
            decoded_pgresponse = pg_response.content.decode("utf-8")
            pg_response_json = json.loads(json.dumps(xmltodict.parse(decoded_pgresponse)))
            extract_single_eu_page(pg_response_json,pgcounter=pg)
    else:
        extract_single_eu_page(response_json,pgcounter=1)

def process_elsevier(pub,method,keywords,configs):
    print('processing elsevier url')
    # get_params = api_parameters(pub)
    keywords=keywords[0]
    # print(get_params)
    publisher_config = configs

    # Build URL
    # get_url=url_builder(get_params,keywords,publisher=pub)
    # url = f"{publisher_config['base_url']}everything;'{keywords}'&wt=json"
    # url=f"http://api.elsevier.com/content/search/sciencedirect?query={keywords}&apiKey={publisher_config['api_key']}"
    url=f"https://api.elsevier.com/content/search/sciencedirect?query={keywords}&apiKey=7f59af901d2d86f78a1fd60c1bf9426a"

    # url=f"https://api.elsevier.com/content/search/sciencedirect?query={keywords}&apiKey=fefb47be759e4fa8e69f9a6336ebeb5a"

    
    print(url)
    response=send_request(url,method).json()
    print(response)
    total_pages=response['search-results']['opensearch:totalResults']
    itemsPerPage=response['search-results']['opensearch:itemsPerPage']

    for i in range(10):
        # print(response['search-results']['link'])
        for j in response['search-results']['link']:
            # print(j['@ref']) 
            if(j['@ref']=='self'):
                new_url = j['@href']
                response=send_request(new_url,method).json()
                print(response)

with open('api_env.yaml', 'r') as file:
    api_config = yaml.safe_load(file)

# print(api_config['springernature'])

def wrapper_func():


    # Tester code:
    publishers=['gatesopen','plos','rsc','eu_open_research','wellcome','springernature','elsevier']
    # publishers=['gatesopen']
    # publishers=['plos','rsc','wellcome','springernature']


    searchkeywords = ['climate']


    # Step1: Create url
    '''
    Each API has different URL , check for the publisher name and get URL
    '''
    method='GET'

    for i in publishers:
        print(f"=================FOR: {i}=========================================")
        configs = api_config[i]
        if i == 'springernature':
            process_sringnature(i,method,searchkeywords,configs)
        elif i =='gatesopen':
            process_gates_open(i,method,searchkeywords,configs)
        elif i=='plos':
            process_plos(i,method,searchkeywords,configs)
        elif i == 'eu_open_research':
            process_open_eu_research(i,method,searchkeywords,configs)
        elif i =='elsevier':
            process_elsevier(i,method,searchkeywords,configs)
            

        
        

        

    #     print(get_url)
    #     if get_url!=0: #solr queries
    #         # send_request
    #         response=send_request(get_url,method)
    #         ''' need to ensure multi page requests are handled and merged'''
    #         # print(response['records'])
    #         content = extract_content(i,response)

    #     else:
    #         if get_url!=0:
    #             response=send_request(get_url,method)
    #             ''' need to ensure multi page requests are handled and merged'''

    #             # print(response)





    # Step 2: send request

    # Step 3: parse

    # Step 4: merge

    # Step 5 : store in S3

wrapper_func()