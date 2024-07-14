import pandas as pd
from tiquu_pg import select_query,insert_query
from hash_func import hashify
import requests
from PyPDF2 import PdfReader
from api_request.send_requests import send_request
import os
from datetime import datetime


def process_sringnature(pub,method,keywords,configs):

    def extract_single_page(response,**kwargs):
        articles = response["records"]
        data_df = pd.DataFrame(columns=['Title','DOI','Authors','Full_Text_URL','Full_Text_Content'])
        for article in articles:
            title = article.get("title")    
            doi = article.get("doi")
            print('for doi -> ',doi)
            # duplicayc check code (handle this in in adifferent func)

            calc_hash = hashify(doi+' '+title)
            result = select_query.get_records('SELECT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])

            print('db_result is ',result)

            if (result==0):
                result = insert_query.insert_record('INSERT',hash_value=calc_hash,schema='web_crawler_journals',journal_name=publisher_config['pub_name'])
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
    # keywords=keywords[0]
    url_keyword_param = '%22'+keywords[0].replace(' ','%20')+'%22'
    # print(get_params)
    publisher_config = configs

    # Build URL
    # get_url=url_builder(get_params,keywords,publisher=pub)
    url = f"{publisher_config['base_url']+publisher_config['collection']}/{publisher_config['result_format']}?api_key={publisher_config['api_key']}&q=keyword:{url_keyword_param}&p={publisher_config['p']}"
    
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
        
