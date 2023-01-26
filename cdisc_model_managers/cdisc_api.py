import os
import requests
import json


class CDISCAPI():
    def __init__(self, baseURL = "https://library.cdisc.org/api", token = os.getenv("CDISC_LIB_KEY")) -> None:
        self.baseURL = baseURL        
        self.token = token

    def query(self, queryEndpoint = "", queryPath = ""):
        resp = requests.get(
            self.baseURL + queryEndpoint + queryPath, 
            headers={
                'api-key': self.token,
                'Accept': 'application/json'}
        )
        # print(resp.status_code)
        if resp.status_code == 200:
            resp_content = resp.json()            
            if isinstance(resp_content, str):
                return json.loads(resp_content)
            else:
                return resp_content
        else:
            return resp.status_code

#cdiscapi = CDISCAPI()            
#resp = cdiscapi.query("/mdr/bc/packages")
#resp = cdiscapi.query("/mdr/bc/packages/2022-10-26/biomedicalconcepts")
#resp = cdiscapi.query("/mdr/bc/packages/2022-10-26/biomedicalconcepts/C25298")