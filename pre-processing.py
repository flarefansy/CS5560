#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 17:49:43 2017

@author: changqunsun
"""

import pandas as pd
import numpy as np
from pandas import Series,DataFrame
data = []
data_state = []
newData = []
data_2017 = []
state_name = ['AK']
last_state = 'AK'


last_value_tar = 0
value_tar = 0
data_tar = []
state_tar = []
last_value_m = 0
value_m = 0
data_m = []
state_m = []
last_value_n = 0
value_n = 0
data_n = []
state_n = []


# generate target data with dataframe form
data = pd.DataFrame(pd.read_csv('dataset/State_Drug_Utilization_Data_2017.csv',na_filter=False,dtype=object))
data_state = pd.DataFrame(data.pop('State'))

data_tar = pd.to_numeric(data.pop('Total Amount Reimbursed'))
data_state.insert(1,'Total Amount Reimbursed',data_tar)
data_tar = pd.to_numeric(data.pop('Medicaid Amount Reimbursed'))
data_state.insert(2,'Medicaid Amount Reimbursed',data_tar)
data_tar = pd.to_numeric(data.pop('Non Medicaid Amount Reimbursed'))
data_state.insert(3,'Non Medicaid Amount Reimbursed',data_tar)

newData = DataFrame(data_state.sort_values(['State'], axis=0, ascending=True)).fillna(0)

#generate list of state name
for i in newData.index:
    if newData['State'][i] != last_state: 
        last_state = newData['State'][i]
        state_name.append(last_state)         
        state_tar.append(value_tar)   
        value_tar = 0
        state_m.append(value_m)   
        value_m = 0
        state_n.append(value_n)   
        value_n = 0        
    else:
        last_value_tar = newData['Total Amount Reimbursed'][i]
        value_tar = last_value_tar + value_tar
        last_value_m = newData['Medicaid Amount Reimbursed'][i]
        value_m = last_value_m + value_m
        last_value_n = newData['Non Medicaid Amount Reimbursed'][i]
        value_n = last_value_n + value_n
#        

data_2017_t, data_2017_m, data_2017_n =[],[],[]
data_2017_t = pd.DataFrame(state_name)
data_2017_m = pd.DataFrame(state_name)
data_2017_n = pd.DataFrame(state_name)

data_2017_t.insert(1,'a',pd.DataFrame(state_tar))
data_2017_m.insert(1,'b',pd.DataFrame(state_m))
data_2017_n.insert(1,'c',pd.DataFrame(state_n))

data_2017_t.columns=['State','Total Amount Reimbursed']
data_2017_m.columns=['State','Medicaid Amount Reimbursed']
data_2017_n.columns=['State','Non Medicaid Amount Reimbursed']

data_dic_t = data_2017_t.set_index('State').T.to_dict('list')
data_dic_m = data_2017_m.set_index('State').T.to_dict('list')
data_dic_n = data_2017_n.set_index('State').T.to_dict('list')

doc = open('DrugU2017_total.txt','w')
for key in data_dic_t.keys():
    print('{name:','\''+key+'\',','value:',data_dic_t[key][0],'},',file=doc)
doc.close()
data_2017_t.to_csv('DrugU2017_total.csv')

doc = open('DrugU2017_medicaid.txt','w')
for key in data_dic_m.keys():
    print('{name:','\''+key+'\',','value:',data_dic_m[key][0],'},',file=doc)
doc.close()
data_2017_m.to_csv('DrugU2017_medicaid.csv')

doc = open('DrugU2017_nonmedicaid.txt','w')
for key in data_dic_n.keys():
    print('{name:','\''+key+'\',','value:',data_dic_n[key][0],'},',file=doc)
doc.close()
data_2017_n.to_csv('DrugU2017_nonmedicaid.csv')