from olMEGA_DataService_Client import olMEGA_DataService_Client
import urllib3
import os
import sys
from datetime import datetime
import requests


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

user = "Mustermann"
password = "12345"
#host = "139.13.250.201"
#client = olMEGA_DataService_Client.client(user, password, host, port=443)
host = "127.0.0.1"
client = olMEGA_DataService_Client.client(user, password, host)
client.silent = True

#pathList = ["/media/sven/Backup/Work/olMEGA/IHAB_converted_EMA2018/"]
pathList = ["/media/sven/Backup/Work/olMEGA/tmp"]
#pathList = ["/home/sven/tmp/EK06DI26_180905_aw/"]

subjectOld = ""
subject = ""

for path in pathList:
    for folder in os.listdir(path):
        for root, dirs, files in os.walk(os.path.join(path, folder)):
            try:
                for file in os.listdir(root):
                    subject = folder.split("_")[0]
                    if subject != subjectOld:
                        print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        print (path)
                        print (subject)
                        subjectOld = subject
                    if not file.lower().startswith("hoersituation-v0"):
                        client.addFileToUpLoad(subject, os.path.join(root, file))
                client.uploadFiles(overwrite = False)
            except requests.exceptions.ConnectionError as e:
                raise e
            except:
                exctype, value = sys.exc_info()[:2]
                print(file)
                print (exctype, value)
                pass

print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))    
