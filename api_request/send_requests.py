

import requests

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
        # print("Error --> ",e,' ',type(e),e.args[0])
        # print(received.text)
        error = "ERROR: "+str(e)+' for '+input_url
        return error
    
