import pandas as pd
from tiquu_pg import select_query,insert_query
from hash_func import hashify
import requests
from api_request.send_requests import send_request
import os
from datetime import datetime
import os
import xml.etree.ElementTree as ET 
import xmltodict
import json

def process_open_eu_research(pub,method,keywords,configs):


    def extract_single_eu_page(response,**kwargs):
        print('here')
        # articles = response["records"]
        data_df = pd.DataFrame(columns=['DOI','Full_Text_url','Full_Text_Content'])

        for doi in response_json['results']['doi']:
            print('doi is ',doi)
            calc_hash = hashify(doi)
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