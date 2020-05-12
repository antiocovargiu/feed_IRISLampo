# Alimentazione di IRIS
# logica di funzionamento: il programma legge le variabili di ambiente e può eseguire l'alimentazione in diverse modalità
# a. diretta: non ci sono ancora dati e vengono acquisiti direttamente dal REM (datainizio e datafine coincidono)
# b. in recupero: possono esserci delle lacune che vengono identificate e il dato corrispondente richiesto al REMWS_GATEWAY
#    in questo caso il recupero va da datainizio a datafine
# 1. inizializzazione
# 2. richiesta elenco sensori al db (df_sensori)

# FASE 1
#inizializzazione
import os,sys
import pandas as pd
# import numpy as np
from sqlalchemy import *
from sqlalchemy import  create_engine
import datetime as dt
import json as js
import requests
import logging



AUTORE=os.getenv('COMPUTERNAME')
if AUTORE is None:
    AUTORE=os.getenv('NAME')


#db paths
IRIS_USER_ID=os.getenv('IRIS_USER_ID')
IRIS_USER_PWD=os.getenv('IRIS_USER_PWD')
IRIS_DB_NAME=os.getenv('IRIS_DB_NAME')
IRIS_DB_HOST=os.getenv('IRIS_DB_HOST')

IRIS_TABLE_NAME='m_osservazioni_tr'
IRIS_SCHEMA_NAME='realtime'

h=os.getenv('TIPOLOGIE') # elenco delle tipologie da cercare nella tabella delle osservazioni realtime, è una stringa
#h="ZTD ZWD GE GN"
TIPOLOGIE=h.split()

# url remws
REMWS_GATEWAY=os.getenv('REMWS_GATEWAY')
url=REMWS_GATEWAY    



#test!!!!!!!
# Livelli di verbosita'
DEBUG=False
DEBUGV=False
levDEBUG=1

if os.getenv('levDEBUG') is not None:
    levDEBUG=int(os.getenv('levDEBUG'))

if os.getenv('DEBUG') is not None:
    DEBUG=eval(os.getenv('DEBUG'))


if (levDEBUG==1):
    DEBUG=True
if (levDEBUG>1):
    DEBUG=True
    DEBUGV=True

################################
#Varibili per effetture un INSERT o un UPDATE (o nessuno dei due)
INSERT = True
UPDATE = False

SINSERT=os.getenv('INSERT')
if SINSERT == 'n':
    INSERT=False


TEST=os.getenv('TEST')
if TEST == 'Y':
    #IRIS_DB_NAME='iris_devel'
    #INSERT=False
    #UPDATE=False
    DEBUGV=True
    #dataTest="202004081225" 

SUPDATE=os.getenv('UPDATE')
if SUPDATE == 'Y':
    INSERT=False
    UPDATE=True 
    

#############################################################################
# init variabili temporali
if os.getenv('MINUTES') is not None:
    MINUTES=int(os.getenv('MINUTES')) #il valore viene sovrascritto dalla variabile d'ambiente (paramentro in launch_feed.sh)
else:
    MINUTES = 240
 
if os.getenv('DELTAT') is not None:
    DELTAT=int(os.getenv('DELTAT')) #il valore viene sovrascritto dalla variabile d'ambiente (paramentro in launch_feed.sh)
else:
    DELTAT = 125
     

# minuto di approssimazione dell'orario (5 o 10)
minM = 5
# formati time
formatTimeC="%Y%m%d%H%M"
formatTime="%Y-%m-%d %H:%M"
formatTimeS="%Y-%m-%d %H:%M:%S"


# se non fornisci una data, piglia l'orario attuale 
#data = dataTest
data = os.getenv('DATATEST')
if data is None:
    datarif=dt.datetime.utcnow()+dt.timedelta(hours=1)
else: 
    datarif=dt.datetime.strptime(data,formatTimeC)
##############################################################################

datafine=datarif-dt.timedelta(minutes=MINUTES)
datainizio=datafine-dt.timedelta(minutes=DELTAT)
if (DEBUG):
	print("datafine",datafine)



#definizione delle funzioni
# la funzione legge il blocco di dati e lo trasforma in DataFrame

def seleziona_richiesta(Risposta):
    # definisco dataframe della risposta
    df_risposta=pd.DataFrame(columns=['data_e_ora','misura','validita'])
    # dizionario di appoggio con la selezione dei dati
    aa=Risposta['data']['sensor_data_list'][0]['data']
    # ciclo lettura
    for i in range(1,len(aa)-1):
        #print(i,aa[i]['datarow'].split(";")[2])
        df_risposta.loc[i-1]=[aa[i]['datarow'].split(";")[0],aa[i]['datarow'].split(";")[1],aa[i]['datarow'].split(";")[2]]
    return df_risposta

def Inserisci_in_realtime(schema,table,idsensore,tipo,operatore,datar,misura,autore):
    # la funzione crea la query da per l'inserimento del dato
    s=dt.datetime.now()
    mystring=s.strftime(formatTime)
    Query_Insert="INSERT into "+schema+"."+table+\
    " (idsensore,nometipologia,idoperatore,data_e_ora,misura, autore,data)\
    VALUES ("+str(idsensore)+",'"+tipo+"',"+str(operatore)+",'"+\
    datar.strftime("%Y-%m-%d %H:%M")+"',"+str(misura)+",'"+ autore+"','"+mystring+"');"
    return Query_Insert


def Update_in_realtime(schema,table,idsensore,tipo,operatore,datar,misura,autore):
    # la funzione crea la query per update di dati
    s=dt.datetime.now()
    mystring=s.strftime(formatTime)
    Query_Update="UPDATE "+schema+"."+table+\
    " SET misura="+str(misura)+\
	" WHERE idsensore="+str(idsensore)+ " AND data_e_ora='"+datar.strftime(formatTimeS)+"';"
    return Query_Update



def Richiesta_remwsgwy (framedati):
    #funzione di colloquio con il remws: manda la dichiesta e decodifica la risposta
    richiesta={
        'header':{'id': 10},
        'data':{'sensors_list':[framedati]}
        }
    ci_sono_dati=False
    try:
       r=requests.post(url,data=js.dumps(richiesta),timeout=5)
       if(len(r.text)>0):
          risposta=js.loads(r.text)
          #controllo progressivamente se la risposta è buona e se ci sono dati
          outcome=risposta['data']['outcome']
          if (outcome==0):
            if (len(risposta['data']['sensor_data_list'])>0):
                candidate=risposta['data']['sensor_data_list'][0]['data']
                for j in candidate:
                    k=j['datarow'].split(";")
                    if (len(k)==3):
                        ora=k[0]
                        misura=k[1]
                        valido=k[2]
                        if(int(valido)>=0):
                            ci_sono_dati=True
                 # chiude ciclo esame dati
       else:
            return []
    except:
        print("Errore: REMWS non raggiungibile", end="\r")
    
    if(ci_sono_dati):
        # estraggo il dato
        return candidate
    else:
        return []
###
#FASE 2 - query al dB
engine = create_engine('postgresql+pg8000://'+IRIS_USER_ID+':'+IRIS_USER_PWD+'@'+IRIS_DB_HOST+'/'+IRIS_DB_NAME)
conn=engine.connect()

#preparazione dell'elenco dei sensori
Query='Select *  from "dati_di_base"."anagraficasensori" where "anagraficasensori"."datafine" is NULL and idrete in (1,2,3,4);'
df_sensori=pd.read_sql(Query, conn)
total_rows = df_sensori.shape[0]

#ALIMETAZIONE DIRETTA
# suppongo di non avere ancora chiesto dati, vedo quale dato devo chiedere, lo chiedo e lo inserisco.
# Se l'inserimento fallisce vuol dire che qualcun altro ha inserito il dato (ovvero un processo in parallelo, il che è strano...)

# selezione dell'ora con approssimazione del minuto secondo minM
minuto=int(datainizio.minute/minM)*minM
minuto_fin=int(datafine.minute/minM)*minM

data_ricerca=dt.datetime(datainizio.year,datainizio.month,datainizio.day,datainizio.hour,minuto,0)
data_ricerca_fin=dt.datetime(datafine.year,datafine.month,datafine.day,datafine.hour,minuto_fin,0)
if (DEBUG):
    print('Date Rischieste al REMws:')
    print('inizio:', data_ricerca)
    print('fine:',data_ricerca_fin)

ora=dt.datetime(datainizio.year,datainizio.month,datainizio.day,datainizio.hour,0,0)

df_section=df_sensori[df_sensori.nometipologia.isin(TIPOLOGIE)].sample(frac=1)
# aggiunto sort casuale per parallelizzazione

#ciclo sui sensori:
# strutturo la richiesta
id_operatore=1
function=1
frame_dati={}
frame_dati["sensor_id"]=0
frame_dati["granularity"]=1 # chiedo solo i 10 minuti

frame_dati["start"]=data_ricerca.strftime(formatTime)
frame_dati["finish"]=data_ricerca_fin.strftime(formatTime)

#suppongo che in df_section ci siano solo i sensori che mi interessano e faccio il ciclo di richiesta
s=dt.datetime.now()
conn=engine.connect()
regole={}
# inizio del ciclo vero e proprio
idx=0
for row in df_section.itertuples():
    idx+=1
    if (DEBUG):
        print(idx,'/',total_rows) 
    # controllo quanto tempo è passato: le alimentazioni possono durare al massimo 10'
    timeDiff=dt.datetime.now()-s
    durata_script=timeDiff.total_seconds() / 60
    if (durata_script>10):
        print("Py Esecuzione troppo lunga - interrompo!")
        sys.exit("Esecuzione troppo lunga - interrompo!")
    frame_dati["sensor_id"]=row.idsensore
    data_insert=data_ricerca

    # assegno operatore e funzione corretti
    # riepilogo casi:
    # frequenza 1 minuti (pluviometro CAE): function=3, idperiodo=1
    # frequenza 5 minuti (pluviometri ETG): function=3, idperiodo=1
    # frequenza 10 minuti (pluviometri PA): function=1, idperiodo=1
    #
    # selezione degli idrometri con frequenza 5 minuti
    # sensori Lampo
    #        frequenza 5 minuti,  function=1, idperiodo=10 (uguale a idrometri a 5 min?)

     
    
    frequenza = row.frequenza    
    if(frequenza==60):
        id_periodo=3
        frame_dati["start"]=ora.strftime(formatTime)
        frame_dati["finish"]=ora.strftime(formatTime)

    else:
    # selezione degli idrometri con frequenza 5 minuti
        if (frequenza==5):
             id_operatore=1
             id_periodo=10                 
        else:
             function=1
             id_periodo=1
    
    if(row.nometipologia=='PP'):
        id_operatore=4
        function=3
        if(row.frequenza>5):
            function=1
    else:
         id_operatore=1
         function=1
           
    frame_dati["operator_id"]=id_operatore
    frame_dati["function_id"]=function
    frame_dati["granularity"]=id_periodo

    if (DEBUGV):
        print('Richiesta remwsgwy', frame_dati)
 
   
    aa=Richiesta_remwsgwy(frame_dati)
    lenaa = len(aa)
    if (DEBUGV):
        print('Lunghezza risposta remwsgwy', lenaa)

    if (lenaa>2):
        for i in range(1, lenaa-1):
            if (len(aa[i]['datarow'].split(";")) == 3):       
                dataREM,misura,valido = aa[i]['datarow'].split(";")
                data_insert = dt.datetime.strptime(dataREM,formatTimeS)
                if (DEBUGV):
                    print('Data da REM:',dataREM)
                    print('Misura da REM:',misura)
                    print('Validita da REM:',valido)      
            
                if (INSERT):         
                    QueryInsert=Inserisci_in_realtime(IRIS_SCHEMA_NAME,IRIS_TABLE_NAME,\
                    row.idsensore,row.nometipologia,id_operatore,data_insert,misura,AUTORE)
                    try:
                        conn.execute(QueryInsert)
                        if (DEBUG):
                            print("+++",row.idsensore,data_insert,misura)
                    except:
                        if (DEBUG):
                            print("Insert non riuscita! per ",row.idsensore)

                if (UPDATE):
                    QueryUpdate=Update_in_realtime(IRIS_SCHEMA_NAME,IRIS_TABLE_NAME,\
                    row.idsensore,row.nometipologia,id_operatore,data_insert,misura,AUTORE)
                    print("Update:")
                    if (DEBUGV):
                        print(QueryUpdate)
                        print("")
                    
                    try:
                        conn.execute(QueryUpdate)
                        if (DEBUG):
                            print("+++",row.idsensore,data_insert,misura)
                    except:
                        if (DEBUG):
                            print("Update non riuscito! per ",row.idsensore)                        
 
    else:
        if (DEBUG):
            print ("!!!! Attenzione: dato di ",TIPOLOGIE, "sensore ", row.idsensore,data_insert, "ASSENTE nel REM")
    
    
    #fine ciclo sensore
print("Alimentazione terminata per ",TIPOLOGIE,", INSERT in IRIS:",INSERT,", inizio:",s.strftime(formatTimeS),", fine:", dt.datetime.now().strftime(formatTimeS))

 
