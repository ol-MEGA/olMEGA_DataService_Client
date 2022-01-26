clear; close all; clc;
addpath('olMEGA_DataService_Client');

myEmaClient = olMEGA_DataService_Client();
Conditions = struct();

%% Get Data
%data = myEmaClient.getDataSet("FeatureSet");

%% Get subset of data (e.g.: Datasets of some Subjects)
Conditions.subject = 'Name';
data = myEmaClient.getDataSet("datachunk", Conditions);

%% Get Data via SQL
% data = myEmaClient.getDataSQL("FeatureSet");

%% Upload Questinares
%data = myEmaClient.uploadQuestinares("Questinares");

%% Upload FeatureFiles
%matlabData = myEmaClient.uploadFiles("./FeatureFiles");

%% Create new FeatureValue
% data = myEmaClient.getDataSet("FeatureSet", Conditions);
% newValue = myEmaClient.createNewFeatureValue("RMS");
% newValue.valueleft = .5;
% newValue.valueright = .12;
% data(1).featurevalue = myEmaClient.pushNewFeatureValue(data(1).featurevalue, newValue);
% data = myEmaClient.updateDataSet(data);

%% delete Data
% myEmaClient.deleteFeatureValues(data);

%% download FeatureFiles
% data = myEmaClient.downloadFiles("out", data);

%% Important: close current connection
myEmaClient.close();
