from os import getenv 
from requests import post
from json import dumps, loads
from dotenv import load_dotenv

load_dotenv()

ORG_LIST = loads(getenv('ORG_LIST'))
EXCEPTION_LIST = loads(getenv('EXCEPTION_LIST'))
SIGVIEW_EMAIL = getenv('SIGVIEW_EMAIL')
SIGVIEW_PASSWORD = getenv('SIGVIEW_PASSWORD')
GET_TOKEN_URL = getenv('GET_TOKEN_URL')
GET_DATE_RANGE_URL = getenv('GET_DATE_RANGE_URL')


def get_date_range():

    headers = {"content-type": "application/json;charset=UTF-8"}
    data = {"email": SIGVIEW_EMAIL, "password": SIGVIEW_PASSWORD , "rememberMe": True}
    data = dumps(data)
    response = post(url=GET_TOKEN_URL, headers=headers, data=data)
    response = response.content.decode("utf-8")
    response = loads(response)
    token = response["token"]
    data_lag = []
 
    headers = {
        "X-Auth-Token": token,
        "Content-Type": "application/json;charset=UTF-8"
    }
    for org, view in ORG_LIST:
        org_view = org+"_"+view
        if org_view not in EXCEPTION_LIST:
            data = {"organization":org,"view":view}
            data = dumps(data)
            try:
                response = post(url=GET_DATE_RANGE_URL, headers=headers, data=data)
                response = response.content.decode("utf-8")
                response = loads(response)
                data_lag.append(int(response["result"]["endDate"]))
            except:
                print(f'You dont have access to {org} and {view}')
                return None
    return data_lag