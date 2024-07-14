import pandas as pd
from tiquu_pg import select_query,insert_query
from hash_func import hashify
import requests
from api_request.send_requests import send_request
import os
from datetime import datetime


def process_plos(pub,method,keywords,configs):

    def extract_single_plos_page(response,**kwargs):
        data_df = pd.DataFrame(columns=['DOI','Full_Text_url','Full_Text_Content'])
        for dois in response['response']['docs']:
            page_doi=dois['id']
            print('doi is ',page_doi)
            calc_hash=hashify(page_doi)
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