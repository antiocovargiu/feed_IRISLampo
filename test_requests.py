# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 17:45:33 2020

@author: avargiu
"""

import requests
import json as js

print ("Test richiesta remws")
url_remws='http://10.10.0.15:9099'
richiesta = "{'header': {'id': 10}, 'data': {'sensors_list': [{'sensor_id': 11659, 'granularity': 1, 'start': '2020-11-30 05:00', 'finish': '2020-11-30 05:00', 'operator_id': 1,'function_id' : 1}]}}"
data = js.dumps(richiesta)
print(data)
r = requests.post(url_remws,data=js.dumps(richiesta))
print (r.status_code)

if r:
    print('Success!')
    risposta=js.loads(r.text)
    print ('risposta: ',risposta)
    
else:
    print('An error has occurred.')


print ("Test sito Arpa")    
urlArpa = 'https://www.arpalombardia.it/Pages/ARPA_Home_Page.aspx'
r=requests.get(urlArpa,timeout=5)

print (r.status_code)
#risposta=js.loads(r.text)
if r:
    print('Success!')
else:
    print('An error has occurred.')
