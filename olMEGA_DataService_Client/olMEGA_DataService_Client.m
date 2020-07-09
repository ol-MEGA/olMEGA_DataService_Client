function handles = olMEGA_DataService_Client(user, password, host)

    if nargin < 3
        disp("Usage:")
        disp("    handles = olMEGA_DataService_Client(user, password, host);")
        return
    end
    tmpDir = pwd;
    cd (fileparts(mfilename('fullpath')));
    PyEMAClientModule = py.importlib.import_module('olMEGA_DataService_Client');
    py.importlib.reload(PyEMAClientModule);
    PyEMAClientObject = py.olMEGA_DataService_Client.olMEGA_DataService_Client.client(user, password, host, 1);
    cd (tmpDir)
    
    handles.getDataSet = @getDataSet;
    handles.exists = @exists;
%     handles.updateDataSet = @updateDataSet;
    handles.createNewFeatureValue = @createNewFeatureValue;
    handles.saveFeatureValue = @saveFeatureValue;
    handles.uploadFiles = @uploadFiles;
    handles.uploadQuestinares = @uploadQuestinares;
    handles.downloadFiles = @downloadFiles;
    handles.deleteFeatureValues = @deleteFeatureValues;
    handles.close = @close;
    handles.pushNewFeatureValue = @pushNewFeatureValue;

    function data = getDataSet(tableName, conditions)
        if nargin < 2
            conditions = false;
        else
            conditions = jsonencode(conditions);
        end
        data = jsondecode(char(PyEMAClientObject.getDataSet(tableName, conditions)));
    end

    function data = exists(tableName, conditions)
        if nargin < 2
            conditions = false;
        else
            conditions = jsonencode(conditions);
        end
        data = PyEMAClientObject.exists(tableName, conditions).double;
    end

%     function data = getDataSQL(tableName, where)
%         if nargin < 2
%             where = "";
%         end
%         data = jsondecode(char(PyEMAClientObject.getDataSQL(tableName, where)));
%     end

%     function data = updateDataSet(dataSet)
%         data = jsondecode(char(PyEMAClientObject.updateDataSet(jsonencode(dataSet))));
%     end

    function data = createNewFeatureValue(FeatureName)
        try
            data = char(PyEMAClientObject.createNewFeatureValue(FeatureName));
            data = jsondecode(data);
        catch
            disp(data)
        end
    end

    function data = saveFeatureValue(Value)
        data = jsondecode(char(PyEMAClientObject.saveFeatureValue(jsonencode(Value))));
    end

    function data = uploadFiles(folder)
        disp("Please wait...");
        data = jsondecode(char(PyEMAClientObject.uploadFiles(folder)));
    end

    function data = uploadQuestinares(folder)
        disp("Please wait...");
        data = jsondecode(char(PyEMAClientObject.uploadQuestinares(folder)));
    end

    function data = pushNewFeatureValue(data, NewFeatureValue)
        if ~isempty(data)
            data(length(data) + 1) = NewFeatureValue;
        else
            data = {NewFeatureValue};
        end
    end

    function data = downloadFiles(outputFolder, dataset)
        disp("Please wait...");
        data = jsondecode(char(PyEMAClientObject.downloadFiles(outputFolder, jsonencode(dataset))));
    end

    function deleteFeatureValues(dataset)
        data = PyEMAClientObject.deleteFeatureValues(jsonencode(dataset));
        if isa(data, 'py.int') && data > 0
            if strcmp('Yes', questdlg({sprintf('The delete command will affect %s FeatureValues!', data) 'Are you sure to delete this data?'}, 'Waring', 'Yes','No', 'No'))
                PyEMAClientObject.deleteFeatureValues(jsonencode(dataset));
            end
        end
    end

    function close()
        PyEMAClientObject.close();
    end
end