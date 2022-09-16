import requests
import logzero
from logzero import logger
import base64
import os
import json

#oracle_url="https://efow.fs.us2.oraclecloud.com"
#oracle_url="https://efow-test.fa.us2.oraclecloud.com"
oracle_url= "https://efow-dev1.fa.us2.oraclecloud.com"


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logs_path = os.path.join(ROOT_DIR, "Logs")
os.makedirs(logs_path, exist_ok=True)
data_path = os.path.join(ROOT_DIR, "Data")
os.makedirs(data_path, exist_ok=True)
logzero.logfile(f"{logs_path}\\functions.log")
logzero.loglevel(logzero.INFO)
logzero.json()

def Get_Ledger_Status(Period):
    entities = get_entities()
    try:
        for i in entities.keys():
            ledger_status = verify_closed_periods(Period,entities[i]["Ledger"])
            ledger_json=json.loads(ledger_status.text)
            ledger_json = ledger_json["items"][0]
            print(ledger_json)
            if ledger_json["ClosingStatus"] == "C":
                return "Error : Some ledgers are closed"
        return "0"
    except:
        return "Error : Not Well formed data"
        
        

def verify_closed_periods(Period,Ledger_id):
    url = f"{oracle_url}/fscmRestApi/resources/11.13.18.05/accountingPeriodStatusLOV?q=PeriodNameId={Period};ApplicationId=101;LedgerId={Ledger_id}"
    print(url)
    headerss = {"Content-Type": "application/json",
            "Connection": "Keep-Alive"
            }
    user="vsethi"
    passw="Welcome1"
    return requests.get(url, auth=(user, passw), headers=headerss)


def read_credentials():
    logger.info("Reading Credentials")
    usr_psw = open(f"{ROOT_DIR}\\Data\\credentials.txt","rb")
    user = base64.b64decode(usr_psw.readline().decode()).decode()
    passw = base64.b64decode(usr_psw.readline().decode()).decode()
    usr_psw.close()
    #logger.info(f"Sending u,p {user},{passw}")
    #print(user,passw)
    return user,passw



def get_ledgers_info():
    url = f"{oracle_url}/fscmRestApi/resources/11.13.18.05/ledgersLOV?limit=10000"
    print(url)
    headerss = {"Content-Type": "application/json",
            "Connection": "Keep-Alive"
            }
    usr_psw = open(f"{ROOT_DIR}\\Data\\credentials.txt","rb")
    user = base64.b64decode(usr_psw.readline().decode()).decode()
    passw = base64.b64decode(usr_psw.readline().decode()).decode()
    return requests.get(url, auth=(user, passw), headers=headerss)




def get_entities():
    ledgers= {}
    ledgers_info = get_ledgers_info()
    ledger_json=json.loads(ledgers_info.text)
    ledger_json = ledger_json["items"]
    for i in ledger_json:
        ledgers[i["Name"]] = i["LedgerId"]
    return  {
        "10": {"Name": "GW US PL", "Ledger":  ledgers["GW US PL"]},
        "12": {"Name": "GW US PL", "Ledger":  ledgers["GW US PL"]},
        "14": {"Name": "GW US PL", "Ledger":  ledgers["GW US PL"]},
        "15": {"Name": "GW US PL", "Ledger":  ledgers["GW US PL"]},
        "16": {"Name": "GW CA PL", "Ledger":  ledgers["GW CA PL"]},
        "65": {"Name": "GW BR PL", "Ledger":  ledgers["GW BR PL"]},
        "41": {"Name": "GW UK PL", "Ledger":  ledgers["GW UK PL"]},
        "43": {"Name": "GW FR PL", "Ledger":  ledgers["GW FR PL"]},
        "45": {"Name": "GW DE PL", "Ledger":  ledgers["GW DE PL"]},
        "46": {"Name": "GW IE PL", "Ledger":  ledgers["GW IE PL"]},
        "47": {"Name": "GW IE PL", "Ledger":  ledgers["GW IE PL"]},
        "48": {"Name": "GW IE PL", "Ledger":  ledgers["GW IE PL"]},
        "49": {"Name": "GW IT PL", "Ledger":  ledgers["GW IT PL"]},
        "51": {"Name": "GW PL PL", "Ledger":  ledgers["GW PL PL"]},
        "55": {"Name": "GW CH PL", "Ledger":  ledgers["GW CH PL"]},
        "57": {"Name": "GW ES PL", "Ledger":  ledgers["GW ES PL"]},
        "71": {"Name": "GW AU PL", "Ledger":  ledgers["GW AU PL"]},
        "72": {"Name": "GW AU PL", "Ledger":  ledgers["GW AU PL"]},
        "80": {"Name": "GW CN PL", "Ledger":  ledgers["GW CN PL"]},
        "86": {"Name": "GW JP PL", "Ledger":  ledgers["GW JP PL"]},
        "84": {"Name": "GW IN PL", "Ledger":  ledgers["GW IN PL"]},
        "85": {"Name": "GW IN PL", "Ledger":  ledgers["GW IN PL"]},
        "82": {"Name": "GW MY PL", "Ledger":  ledgers["GW MY PL"]},
        "61": {"Name": "GW AR PL", "Ledger":  ledgers["GW AR PL"]},
        "53": {"Name": "GW AT PL", "Ledger":  ledgers["GW AT PL"]},
        "58": {"Name": "GW DK PL", "Ledger":  ledgers["GW DK PL"]},
    }
