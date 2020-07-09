# How To

Anwendung: siehe Beispiele

Folgende DataSets können mittels "getDataSet()" geladen werden:
- FeatureSet
  enhält: Featurevalues, Files, Questionnaires
- FileType
  enthält: Feature_Filetypes, Files
- Feature
  enthält: Feature_Filetypes, Featurevalues
- Questionnaire
  enthält: Answers
- Question
  enthält: Answers
- Featurevalue
- File
- Answer

## Selektionskriterien
Als Selektionskriterien ("conditions" in den Beispielen unten) sind alle Felder aus den oben genannten DataSets möglich. Dabei ist die Hierachie entsprechend zu beachten:

### Beispiel "FeatureSet"
conditions = {"subject": "P1"}
-> es werden alle FeatureSet des Probanden P1 gesucht

conditions = {"subject": ["P1", "P2"] }
-> es werden alle FeatureSet der Probanden P1 oder P2 gesucht

{"subject": "P1", "Featurevalue" : {"name" : "rms"}}
-> es werden alle FeatureSet des Probanden P1 mit den enthaltenen Featurevalues "rms" gesucht

{"subject": ["P1", "P2"], "Featurevalue" : {"name" : ["rms", "psd"]}}
-> es werden alle FeatureSet der Probanden P1 oder P2 mit den enthaltenen Featurevalues "rms" oder "psd" gesucht

## Python
### Beispiel: alle Featureset-Daten laden
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    client = olMEGA_DataService_Client.client("Mustermann", "12345", "localhost")
    myDataSet = client.getDataSet("featureset")
    client.close()

### Beispiel: alle Featureset-Daten von einem Probanden laden
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    client = olMEGA_DataService_Client.client("Mustermann", "12345", "localhost")
    conditions = {"subject": "P1"};
    myDataSet = client.getDataSet("featureset", conditions)
    client.close()

### Beispiel: Questinare-XMLs hochladen (im Verzeichnis "./Questinares" mit jeweils ein Ordner pro Proband)
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    client.uploadQuestinares("./Questinares")
    client.close()

### Beispiel: Feature-Dateien hochladen (im Verzeichnis "./FeatureFiles" mit jeweils ein Ordner pro Proband)
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    client.uploadFiles("./FeatureFiles")
    client.close()

### Beispiel: alle Feature-Dateien von einem Probanden herunter laden und im Ordner "./out" speichern
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    client = olMEGA_DataService_Client.client("Mustermann", "12345", "localhost")
    conditions = {"subject": "P1"};
    myDataSet = client.getDataSet("featureset", conditions)
    client.downloadFiles("./out", myDataSet)
    client.close()

### Beispiel: neues Feature für einen Probanden erstellen und speichern
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    conditions = {"subject": "P1"};
    myDataSet = client.getDataSet("featureset", conditions)
    newValue = client.createNewFeatureValue("PSD")
    newValue["valueleft"] = 0.42
    newValue["valueright"] = 0.21
    myDataSet[0]["featurevalue"].append(newValue)
    client.updateDataSet(myDataSet)
    client.close()

### Beispiel: Werte eines Features für einen Probanden ändern und speichern
    from olMEGA_DataService_Client import olMEGA_DataService_Client
    conditions = {"subject": "P1"};
    myDataSet = client.getDataSet("featureset", conditions)
    myDataSet[0]["featurevalue"][0]["valueleft"] = 0.42
    client.updateDataSet(myDataSet)
    client.close()

## Matlab
### Beispiel: alle Featureset-Daten laden
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    myDataSet = jsondecode(char(pyObj.getDataSet("featureset")));
    pyObj.close();

### Beispiel: alle Featureset-Daten von einem Probanden laden
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    Conditions = struct();
    Conditions.Subject = {'P1'};
    myDataSet = jsondecode(char(pyObj.getDataSet("featureset", jsonencode(Conditions))));
    pyObj.close();

### Beispiel: Questinare-XMLs hochladen (im Verzeichnis "./Questinares" mit jeweils ein Ordner pro Proband)
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    pyObj.uploadQuestinares("./Questinares");
    pyObj.close();

### Beispiel: Feature-Dateien hochladen (im Verzeichnis "./FeatureFiles" mit jeweils ein Ordner pro Proband)
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    pyObj.uploadFiles("./FeatureFiles");
    pyObj.close();

### Beispiel: alle Feature-Dateien von einem Probanden herunter laden und im Ordner "./out" speichern
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    % will follow
    pyObj.close();

### Beispiel: neues Feature für einen Probanden erstellen und speichern
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    Conditions = struct();
    Conditions.subject =  {"P1"};
    myDataSet = jsondecode(char(pyObj.getDataSet("featureset", jsonencode(Conditions))));
    newValue = jsondecode(char(pyObj.createNewFeatureValue("PSD")));
    newValue.valueleft = .42;
    newValue.valueright = .21;
    myDataSet(1).featurevalue(length(matlabData(1).featurevalue) + 1) = newValue;
    pyObj.updateDataSet(jsonencode(matlabData));
    pyObj.close();

### Beispiel: Werte eines Features für einen Probanden ändern und speichern
    myMod = py.importlib.import_module('olMEGA_DataService_Client');
    pyObj = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client('Mustermann', '12345', "localhost", 1);
    myDataSet = jsondecode(char(pyObj.getDataSet("featureset", jsonencode(Conditions))));
    myDataSet(1).featurevalue(1).valueleft = .42
    myDataSet(1).featurevalue(1).valueleft = .21
    pyObj.updateDataSet(jsonencode(matlabData));
    pyObj.close();

