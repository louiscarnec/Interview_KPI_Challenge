
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ceeb8840af6d4fdd9d3fd28fc998b7f2)](https://app.codacy.com/app/louis_6/Interview_KPI_Challenge?utm_source=github.com&utm_medium=referral&utm_content=louiscarnec/Interview_KPI_Challenge&utm_campaign=badger)

Code in response to interview challenge as part of data analyst application. 

ANSWERS AT: https://docs.google.com/spreadsheets/d/1iQ2mUYHvPzNyVk6ZxS66mNIvC1vPf99L6qQ59ebAq5o/edit#gid=1891262416

Description
In this case you will find information relating to a SaaS company. Specially the company has been tracking the expenditure of customers (Table 1) and usage and account size data (Table 2).

The company wants to measure recurring revenue and recurring revenue growth, as well as retention of users. The company is also interested to understand what profile of customer is “their core users”.

Below you will find some interesting KPIs to track regarding the interesting areas:

Recurring Revenue
MRR, New MRR (Abs, %), Expansion MRR (Abs, %)
Retention
Churn, MRR downgrades (Abs, %)
Financial Figures
The company spends around 4000 € in customer acquisition per month (both sales and marketing)
Data
Data is provided through two tables, examples are provided below as well as the links to the tables. The format is newline-separated json-object data.

Table 1: Invoice data
Customer ID
Invoice amount (€)
Time invoice (sec)
274354
90.0
1472133061


Table 2 customer info

Customer ID
Nr of web sessions
Nr of seats
274354
11.0
20
Task
The first task is to analyze the growth of the company as well as the retention trends of the company. 
The second task would be to analyze if the company’s growth efforts are being profitable. 
The third task will be, with the available data, to provide a profile of what we could call “the core users” meaning, how do the company’s best customer look?


Since the company would like to have this analysis automatized, it will be a plus (not required) if the data could be pushed to spreadsheets using google scripts. (this last part of the task can be just a script does not need to be integrated)

