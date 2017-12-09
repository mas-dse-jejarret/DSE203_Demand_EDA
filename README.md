# DSE203_Demand_EDA
### Project Description: 
### Given multiple data sources- PostGres, Asterixdb and Solr, the objective is to have end-to-end solution for predicting the demand of Book Sales.</br>
**We are using UCSD data servers set up for us. Make sure to connect VPN, and wifi must be UCSD Protected, not Guest.** </br></br>
The project is divided among four groups: Exploratory Data Analysis(EDA), Query Processing, Schema Integration and Machine Learning.
This repo is set up and used by team EDA.

## What have we achieved ?
Most of our project files are located in folder 'www'. </br>
EDA team has completed about 15 APIs, all organized in one file called 'data_exploration2.py' as different functions so they can be imported and used.</br>
All APIs are also tested as REST API using Flask. You have to first run 'rest_api_server.py' first. Once this file is running and server is started, APIs can be called from localhost browser.
Example:
http://localhost:5000/static/html/index.html </br>
This is our documentation to all the APIs we have. It is well organized and indexed.

## Tools to download ?
Python 2.7 is preferred version of python.
Install Flask within the env where you have Python 2.7</br>
Pip install all the modules as required by 'data_exploration2.py' </br>(Note: When you try to import function from this file, if any module is missing, it will alert you to install that).

## Rest API with HTML files for Visualization
We have following 3 HTML files that can be ran on localhost browser. Make sure 'rest_api_server.py' is running at backend.
- http://localhost:5000/static/level1/benchmark_client.html : Benchmarking and Optimization.
- http://localhost:5000/static/level1/SalesByCategory.html	: Break-down of Sales by month for given Category.
- http://localhost:5000/static/level1/SentimentPolarity.html : Sentiment Polarity for Solr Review Text.

## Tableau Visualization
We have also performed some visualization on Tableau based on the data pulled from Mediated Schema.
https://public.tableau.com/profile/orysya.stus#!/vizhome/EDA_DemandPrediction/Demand 

Please follow our presentation slides and demo which are in respective folders for more information.
