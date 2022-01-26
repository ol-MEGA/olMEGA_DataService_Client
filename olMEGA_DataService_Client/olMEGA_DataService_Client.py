import os
import sys
import requests
import json
from requests.auth import HTTPBasicAuth
import tempfile
import zipfile
import urllib3
import configparser

class client():

    def __init__(self, user = "", password = "", host = "", calledByMatlab = False, port = -1, debug = False):
        if sys.version_info[0] < 3:
            raise Exception("Must be using Python 3")
        config = configparser.ConfigParser()
        if os.path.isfile("settings.conf"):
            config.read('settings.conf')
            if user == "":
                user = config["MAIN"].get("Username", "")
            if password == "":
                password = config["MAIN"].get("Password", "")
            if host == "":
                host = config["MAIN"].get("Host", "127.0.0.1")
            if port == -1:
                port = int(config["MAIN"].get("Port", "443"))
        else:
            config['MAIN'] = {"Username": "", "Password": "", "Host": "127.0.0.1", "Port": 443}
            with open('settings.conf', 'w') as configfile:
                config.write(configfile)
            print("\33[31mInfo: empty settings.conf was created! Please add login informations!\33[0m")
            exit()
        self.auth = HTTPBasicAuth(user, password)
        self.host = "https://" + host + ":" + str(port)
        self.calledByMatlab = calledByMatlab
        if calledByMatlab:
            os.chdir(os.path.abspath(os.path.dirname(__file__)))
        self.verifySSL = False
        self.session = requests.Session()
        self.silent = False
        if debug:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)            

    def close(self):
        try:
            self.session.get(self.host + "/close", auth = self.auth, verify = self.verifySSL)
            self.session.close()
        except:
            pass

    def getDataSet(self, tableName, Conditions = False):
        if self.calledByMatlab and not type(Conditions) is bool:
            Conditions = json.loads(Conditions)
        #if type(Conditions) is dict or type(Conditions) is list:
        #    Conditions = {str(tableName): Conditions}
        data = {"TABLENAME" : str(tableName), "CONDITIONS": Conditions}
        response = self.session.post(self.host + "/getDataset", auth = self.auth, data = json.dumps(data), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            response = json.loads(response.text)
            if self.calledByMatlab:
                response = json.dumps(response)
        elif response.status_code == 500:
            raise ValueError(response.text)
        else:
            response = response.text
        return response
    
    def exists(self, tableName, Conditions = False):        
        if self.calledByMatlab and not type(Conditions) is bool:
            Conditions = json.loads(Conditions)
        #if type(Conditions) is dict or type(Conditions) is list:
        #    Conditions = {str(tableName): Conditions}
        data = {"TABLENAME" : str(tableName), "CONDITIONS": Conditions}
        response = self.session.post(self.host + "/exists", auth = self.auth, data = json.dumps(data), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            if response.text == "True":
                return 1
            else:
                return 0
        elif response.status_code == 500:
            raise ValueError(response.text)
        else:
            response = response.text
        return response

    """
    def getDataSQL(self, tableName, strSQLWhere):
        data = {"TABLE" : str(tableName), "WHERE": strSQLWhere}
        response = self.session.post(self.host + "/getDataSQL", auth = self.auth, data = json.dumps(data), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            response = json.loads(response.text)
            if self.calledByMatlab:
                response = json.dumps(response)
        elif response.status_code == 500:
            raise ValueError(response.text)
        else:
            response = response.text
        return response
    """
    
    """
    def updateDataSet(self, dataSet):
        if self.calledByMatlab:
            dataSet = json.loads(dataSet)
        response = self.session.post(self.host + "/updateDataset", auth = self.auth, data = json.dumps(dataSet), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            response = json.loads(response.text)
            if self.calledByMatlab:
                response = json.dumps(response)
        elif response.status_code == 500:
            raise ValueError(response.text)
        else:
            response = response.text
        return response
    """
    
    def createNewFeatureValue(self, FeatureName):
        data = {"Feature": FeatureName}
        response = self.session.post(self.host + "/createNewFeatureValue", auth = self.auth, data = json.dumps(data), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            response = json.loads(response.text)
            if self.calledByMatlab:
                response = json.dumps(response)
        elif response.status_code == 500:
            raise ValueError(response.text)
        else:
            response = response.text
        return response
    
    def saveFeatureValue(self, value):
        if self.calledByMatlab:
            value = json.loads(value)
        data = {"Value": value}
        response = self.session.post(self.host + "/saveFeatureValue", auth = self.auth, data = json.dumps(data), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            if response.text == "True":
                return 1
            else:
                return 0
        elif response.status_code == 500:
            raise ValueError(response.text)
        else:
            response = response.text
        return response

    def importFiles(self, FileExtension, filedata, overwrite = False):
        def sendData(data, files):
            if len(data) > 0:
                response = False
                print('.', end = '', flush = True)
                if FileExtension.lower() == ".feat":
                    response = self.session.put(self.host + "/importFiles", auth = self.auth, data = {"data": json.dumps(data)}, files = files, verify = self.verifySSL)
                elif FileExtension.lower() == ".xml":
                    response = self.session.put(self.host + "/importQuestionaere", auth = self.auth, data = {"data": json.dumps(data)}, files = files, verify = self.verifySSL)
                for file in files:
                    file[1].close()
                if response and response.status_code == 200:
                    return 1
                else:
                    raise ValueError(response.text)
            else:
                return True
        if type(filedata) is dict and len(filedata) > 0:
            print('uploading', end = '', flush = True)
            if FileExtension == ".feat" and overwrite == False:
                response = self.session.post(self.host + "/exists", auth = self.auth, data = json.dumps(filedata), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
                if response.status_code == 200:
                    filedata = json.loads(response.text) 
            data = {}
            files = []
            datasize = 0
            returnValue = True
            for subject in filedata.keys():
                if type(filedata[subject]) is str:
                    filedata[subject] = [filedata[subject]]
                for file in filedata[subject]:
                    _, file_extension = os.path.splitext(file)
                    if os.path.isfile(file) and file_extension.lower() == FileExtension:
                        data[str(len(data))] = {"subject": subject , "filename" : os.path.basename(file), "overwrite": overwrite}
                        files.append((str(len(files)), (open(file, 'rb'))))
                        datasize += os.path.getsize(file)
                        if datasize >= 500000000 or len(files) >= 1000:
                            returnValue = returnValue and sendData(data, files)
                            data = {}
                            files = []
                            datasize = 0
            returnValue = returnValue and sendData(data, files)
            print("")
            return returnValue
        else:
            if FileExtension.lower() == ".feat":
                raise ValueError("Import impossible! Features Files are missing...")        
            elif FileExtension.lower() == ".xml":
                raise ValueError("Import impossible! Questinares Files are missing...")
    
    """
    def importFiles(self, FileExtension, filedata, overwrite = False):
        def sendData(data, files):
            if len(data) > 0:
                response = False
                print('.', end = '', flush = True)

                tempFile = tempfile.NamedTemporaryFile(mode='w', delete=True)
                zipf = zipfile.ZipFile(tempFile.name, 'w', zipfile.ZIP_DEFLATED)
                for file in files:
                    zipf.write(file)
                zipf.close()
                if FileExtension.lower() == ".feat":
                    response = self.session.put(self.host + "/importFiles", auth = self.auth, data = {"data": json.dumps(data)}, files = {'zip': open(tempFile.name,'rb')}, verify = self.verifySSL)
                elif FileExtension.lower() == ".xml":
                    response = self.session.put(self.host + "/importQuestionaere", auth = self.auth, data = {"data": json.dumps(data)}, files = [tempFile.name], verify = self.verifySSL)
                tempFile.close()
                if response and response.status_code == 200:
                    return 1
                else:
                    raise ValueError(response.text)
            else:
                return True
        if type(filedata) is dict and len(filedata) > 0:
            print('uploading', end = '', flush = True)
            if FileExtension == ".feat" and overwrite == False:
                response = self.session.post(self.host + "/exists", auth = self.auth, data = json.dumps(filedata), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
                if response.status_code == 200:
                    filedata = json.loads(response.text) 
            data = {}
            files = []
            datasize = 0
            returnValue = True
            for subject in filedata.keys():
                if type(filedata[subject]) is str:
                    filedata[subject] = [filedata[subject]]
                for file in filedata[subject]:
                    _, file_extension = os.path.splitext(file)
                    if os.path.isfile(file) and file_extension.lower() == FileExtension:
                        data[str(len(data))] = {"subject": subject , "filename" : os.path.basename(file), "overwrite": overwrite}
                        files.append(file)
                        datasize += os.path.getsize(file)
                        if datasize >= 500000000 or len(files) >= 1000:
                            returnValue = returnValue and sendData(data, files)
                            data = {}
                            files = []
                            datasize = 0
            returnValue = returnValue and sendData(data, files)
            print("")
            return returnValue
        else:
            if FileExtension.lower() == ".feat":
                raise ValueError("Import impossible! Features Files are missing...")        
            elif FileExtension.lower() == ".xml":
                raise ValueError("Import impossible! Questinares Files are missing...")
    """
                
    def uploadFiles(self, overwrite = False):
        returnValue = True
        if hasattr(self, 'temp_FeatureFilesToUpload'):
            returnValue = returnValue and self.importFiles(".feat", self.temp_FeatureFilesToUpload, overwrite)
            del self.temp_FeatureFilesToUpload
        if hasattr(self, 'temp_QuestinaresFilesToUpload'):
            returnValue = returnValue and self.importFiles(".xml", self.temp_QuestinaresFilesToUpload)
            del self.temp_QuestinaresFilesToUpload
        return returnValue
            
    def addFileToUpLoad(self, subject, filename):
        _, file_extension = os.path.splitext(filename)
        if file_extension.lower() == ".feat":
            if not hasattr(self, 'temp_FeatureFilesToUpload'):
                self.temp_FeatureFilesToUpload = {}
            if not subject in self.temp_FeatureFilesToUpload.keys():
                self.temp_FeatureFilesToUpload[subject] = []
            self.temp_FeatureFilesToUpload[subject].append(filename)
        elif file_extension.lower() == ".xml":
            if not hasattr(self, 'temp_QuestinaresFilesToUpload'):
                self.temp_QuestinaresFilesToUpload = {}
            if not subject in self.temp_QuestinaresFilesToUpload.keys():
                self.temp_QuestinaresFilesToUpload[subject] = []
            self.temp_QuestinaresFilesToUpload[subject].append(filename)
        
    def downloadFiles(self, outputFolder, dataset, overwrite = False):
        if not os.path.isdir(outputFolder):
            os.mkdir(outputFolder)
        if self.calledByMatlab:
            dataset = json.loads(dataset)
        dataset = json.dumps(dataset)
        print('downloading', end = '', flush = True)
        while True:
            print('.', end = '', flush = True)
            response = self.session.post(self.host + "/exportFiles", auth = self.auth, data = dataset, headers = {'content-type': 'application/json'}, verify = self.verifySSL)
            if response.status_code == 200:
                dataset = json.dumps({"loadnext" : 1})
                tempFile = tempfile.NamedTemporaryFile(mode='w', delete=False)
                open(tempFile.name, 'wb').write(response.content)
                with zipfile.ZipFile(tempFile.name,"r") as zip_ref:
                    zip_ref.extractall(outputFolder)
                tempFile.close()
            elif response.status_code == 201:
                dataset = json.loads(response.text)
                if overwrite == False:
                    for count in reversed(range(len(dataset))):
                        if os.path.isfile(os.path.join(outputFolder, dataset[count])):
                            del dataset[count]
                dataset = json.dumps({"loadnext" : dataset})
            elif response.status_code == 204:   
                break         
            elif response.status_code == 500:
                raise ValueError(response.text)
            else:
                raise ValueError(response.text)
        print("")
        return True
    
    def executeQuery(self, command):
        data = {"COMMAND" : str(command)}
        response = self.session.post(self.host + "/executeQuery", auth = self.auth, data = json.dumps(data), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
        if response.status_code == 200:
            response = json.loads(response.text)
            if len(response) > 0:
                return response
            else:
                return {}
        else:
            return {}

    def deleteFeatureValues(self, dataset):      
        if self.calledByMatlab:
            dataset = json.loads(dataset)
        if type(dataset) is dict:
            dataset = [dataset]
        if type(dataset) is list:
            response = self.session.post(self.host + "/deleteFeatureValues", auth = self.auth, data = json.dumps(dataset), headers = {'content-type': 'application/json'}, verify = self.verifySSL)
            if response.status_code == 200:
                response = json.loads(response.text)
                if type(response) is dict and len(response.keys()) == 1 and list(response.keys())[0] == "CONFIRMDELETE":
                    if response["CONFIRMDELETE"] > 0:
                        if self.calledByMatlab == False:
                            dialogResult = False
                            try:
                                import tkinter as tk
                                from tkinter import messagebox
                                tk.Tk().withdraw()
                                dialogResult = messagebox.askyesno("Warning", "The delete command will affect " + str(response["CONFIRMDELETE"]) + " FeatureValues!\nAre you sure to delete this data? (yes/no)")
                            except:                
                                print ("The delete command will affect " + str(response["CONFIRMDELETE"]) + " FeatureValues!")
                                dialogResult = input("Are you sure to delete this data? (yes/no) ").lower() in {'yes','y', 'ye'}
                            if dialogResult:
                                self.deleteFeatureValues(dataset)
                        else:
                            return response["CONFIRMDELETE"]
                    else:
                        print ("0 rows to delete!")
                        
                elif type(response) is dict and len(response.keys()) == 1 and list(response.keys())[0] == "ROWSAFFECTED":
                    print ("\n" + str(response["ROWSAFFECTED"]) + " FeatureValues deleted!")
            elif response.status_code == 500:
                raise ValueError(response.text)
        else:
            raise ValueError("Dataset is not valid!");
if __name__ == "__main__":
    exec(open("../example.py").read())