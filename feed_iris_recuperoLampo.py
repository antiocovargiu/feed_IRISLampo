# Alimentazione di IRIS
# logica di funzionamento: il programma legge le variabili di ambiente e può eseguire l'alimentazione in diverse modalità
# a. diretta: non ci sono ancora dati e vengono acquisiti direttamente dal REM (datainizio e datafine coincidono)
# b. in recupero: possono esserci delle lacune che vengono identificate e il dato corrispondente richiesto al REMWS_GATEWAY
#    in questo caso il recupero va da datainizio a datafine
# 1. inizializzazione
# 2. richiesta elenco sensori al db (df_sensori)

# FASE 1
#inizializzazione
import os
import pandas as pd
# import numpy as np
from sqlalchemy import *
import datetime as dt
import json as js
import requests
import logging
# variabili di ambiente (da togliere in produzione)
DEBUG='False'
if os.getenv('DEBUG') is not None:
    DEBUG=os.getenv('DEBUG')

IRIS_TABLE_NAME='m_osservazioni_tr'
IRIS_SCHEMA_NAME='realtime'
AUTORE=os.getenv('COMPUTERNAME')
MINUTES= 1440 # minuti di recupero
if (AUTORE==None):
    AUTORE=os.getenv('NAME')
    IRIS_USER_ID=os.getenv('IRIS_USER_ID')
    IRIS_USER_PWD=os.getenv('IRIS_USER_PWD')
    IRIS_DB_NAME=os.getenv('IRIS_DB_NAME')
    IRIS_DB_HOST=os.getenv('IRIS_DB_HOST')
    h=os.getenv('TIPOLOGIE') # elenco delle tipologie da cercare nella tabella delle osservazioni realtime, è una stringa   
    MINUTES=int(os.getenv('MINUTES'))
    REMWS_GATEWAY=os.getenv('REMWS_GATEWAY')
    # trasformo la stringa in lista

url=REMWS_GATEWAY
TIPOLOGIE=h.split()

# formati time
formatTimeC="%Y%m%d%H%M"
formatTime="%Y-%m-%d %H:%M"
formatTimeS="%Y-%m-%d %H:%M:%S"


# inizializzazione delle date
datafine=dt.datetime.utcnow()+dt.timedelta(hours=1)

if os.getenv('DATARECUP') is not None:
    data = os.getenv('DATARECUP')
    datafine=dt.datetime.strptime(data,formatTimeC)


datainizio=datafine-dt.timedelta(minutes=MINUTES)


DEBUGV=False
if (eval(DEBUG)):
    logging.basicConfig(level=logging.DEBUG)
    DEBUGV=True 
else:    
    logging.basicConfig(level=logging.INFO)


if os.getenv('LISTANERA') is not None:
      lista=os.getenv('LISTANERA') # elenco sensori in lista nera manuale
      whrListaNera='AND idsensore not in ' + lista
else:
      whrListaNera=''

if (DEBUGV):
    print('lista sensori eliminati manualmente: ',lista) 

#definizione delle funzioni
# la funzione legge il blocco di dati e lo trasforma in DataFrame
def seleziona_richiesta(Risposta):
    # definisco dataframe della risposta
    df_risposta=pd.DataFrame(columns=['data_e_ora','misura','validita'])
    # dizionario di appoggio con la selezione dei dati
    aa=Risposta['data']['sensor_data_list'][0]['data']
    # ciclo lettura
    for i in range(1,len(aa)-1):
    #    print(i,aa[i]['datarow'].split(";")[2])
        df_risposta.loc[i-1]=[aa[i]['datarow'].split(";")[0],aa[i]['datarow'].split(";")[1],aa[i]['datarow'].split(";")[2]]
    return df_risposta
def Inserisci_in_realtime(schema,table,idsensore,tipo,operatore,datar,misura,autore):
    # la funzione crea la query da per l'inserimento del dato
    s=dt.datetime.now()
    mystring=s.strftime("%Y-%m-%d %H:%M")
    Query_Insert="INSERT into "+schema+"."+table+\
    " (idsensore,nometipologia,idoperatore,data_e_ora,misura, autore,data)\
    VALUES ("+str(idsensore)+",'"+tipo+"',"+str(operatore)+",'"+\
    datar.strftime("%Y-%m-%d %H:%M")+"',"+str(misura)+",'"+ autore+"','"+mystring+"');"
    return Query_Insert
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
        if(ci_sono_dati):
                # estraggo il dato
                return candidate
        else:
                return []
    except:
        print("Errore: REMWS non raggiungibile", end="\r\n")
        logging.error("REMWSGWY non raggiungibile")
        return []
    
###
#FASE 2 - query al dB
engine = create_engine('postgresql+pg8000://'+IRIS_USER_ID+':'+IRIS_USER_PWD+'@'+IRIS_DB_HOST+'/'+IRIS_DB_NAME)
conn=engine.connect()

#preparazione dell'elenco dei sensori
Query='Select *  from "dati_di_base"."anagraficasensori" where "anagraficasensori"."datafine" is NULL and idrete in (1,2,3,4) '+ whrListaNera +';'
df_sensori=pd.read_sql(Query, conn)

#query di richiesta dati già presenti nel dB
QueryDati='Select * from "'+IRIS_SCHEMA_NAME+'"."'+IRIS_TABLE_NAME+'" where "m_osservazioni_tr"."data_e_ora" between \''+datainizio.strftime("%Y-%m-%d %H:%M")+'\' and \''+datafine.strftime("%Y-%m-%d %H:%M")+'\';'
df_dati=pd.read_sql(QueryDati, conn)

#ALIMETAZIONE IN RECUPERO
# suppongo di avere già dei dati, vedo qule datomanca, lo chiedo e loinserisco.
# Se l'inserimento fallisce vuol dire che qualcun altro ha inserito il dato (ovvero un processo in parallelo, il che è possibilestrano...)

# selezione dell'ora
minuto=int(datainizio.minute/10)*10
data_ricerca=dt.datetime(datainizio.year,datainizio.month,datainizio.day,datainizio.hour,minuto,0)
data_elimina=data_ricerca - dt.timedelta(days=15)
df_section=df_sensori[df_sensori.nometipologia.isin(TIPOLOGIE)]
tot_rows=df_section.shape[0]
if (DEBUGV):
    #print('df_section',df_section)
    print(tot_rows)
    print(TIPOLOGIE)

#ciclo sui sensori:
# strutturo la richiesta
id_operatore=1
function=1
frame_dati={}
frame_dati["sensor_id"]=0
frame_dati["granularity"]=1 # chiedo solo i 10 minuti
frame_dati["start"]=data_ricerca.strftime("%Y-%m-%d %H:%M")
frame_dati["finish"]=data_ricerca.strftime("%Y-%m-%d %H:%M")


logging.info('ricerca dati start: '+ frame_dati["start"])
logging.info('ricerca dati finish: '+ datafine.strftime("%Y-%m-%d %H:%M"))

#suppongo che in df_section ci siano solo i sensori che mi interessano e faccio il ciclo di richiesta
s=dt.datetime.now()
conn=engine.connect()
# inizio del ciclo vero e proprio
idx=0
for row in df_section.itertuples():
    idx+=1

    logging.info('Processing richiesta '+ str(idx) + '/' + str(tot_rows))
    if (eval(DEBUG)):
        logging.debug('nometipologia:'+ row.nometipologia)

    #estraggo i dati dal dataframe
    element=df_dati[df_dati.idsensore==row.idsensore]
    frame_dati["sensor_id"]=row.idsensore
    #frequenza 5 minuti
    if(row.frequenza==60):
        id_periodo=3
        PERIODO=int(MINUTES/60)
        attesi=pd.date_range(dt.datetime(datainizio.year,datainizio.month,datainizio.day,datainizio.hour,0,0), periods=PERIODO,freq='60min')
    else:
        if(row.frequenza==5):
            id_periodo=10
            function=1
            id_operatore=1
            PERIODO=int(MINUTES/5)
            attesi=pd.date_range(data_ricerca, periods=PERIODO,freq='5min')
        else:    
            id_periodo=1
            PERIODO=int(MINUTES/10)
            attesi=pd.date_range(data_ricerca, periods=PERIODO,freq='10min')
    # assegno operatore e funzione corretti
    if(row.nometipologia=='PP'):
        id_operatore=4
        function=3
        id_periodo=1
        if(row.frequenza>5):
            function=1
    else:
         id_operatore=1
         function=1
    #selezione del valore orario se la frequenza è 60
    
    #ho selezionato il periodo atteso: estraggo il dataframe degli elementi attesi
    df=attesi.isin(element['data_e_ora'])
    #eseguo il ciclo di richiesta sui dati mancanti
    for dato_mancante in attesi[~df]:
        frame_dati["start"]=dato_mancante.strftime("%Y-%m-%d %H:%M")
        frame_dati["finish"]=dato_mancante.strftime("%Y-%m-%d %H:%M")
        frame_dati["operator_id"]=id_operatore
        frame_dati["function_id"]=function
        frame_dati["granularity"]=id_periodo
        try:
            aa=Richiesta_remwsgwy(frame_dati)
        except:
            aa=[]
        if (len(aa)>2):
        # prendo solo il primo element
            misura=aa[1]['datarow'].split(";")[1]
            QueryInsert=Inserisci_in_realtime(IRIS_SCHEMA_NAME,IRIS_TABLE_NAME,\
            row.idsensore,row.nometipologia,id_operatore,dato_mancante,misura,AUTORE)
            try:
                conn.execute(QueryInsert)
                if (eval(DEBUG)):
                    logging.debug("+++++++Query eseguita per "+str(row.idsensore)+" "+ dato_mancante.strftime("%Y-%m-%d %H:%M"))
            except:
                if (eval(DEBUG)):
                    logging.error(QueryInsert+"non riuscita! per "+str(row.idsensore)+" "+ dato_mancante.strftime("%Y-%m-%d %H:%M"))
        else:
                logging.warning("Attenzione: dato di "+str(row.idsensore)+ " ASSENTE nel REM per "+ dato_mancante.strftime("%Y-%m-%d %H:%M"))
     # prima di chiudere il ciclo chiedo la raffica del vento
    if(row.nometipologia=='VV' or row.nometipologia=='DV'):
        id_operatore=3         
        frame_dati["operator_id"]=id_operatore
        try:
            aa=Richiesta_remwsgwy(frame_dati)
        except:
            aa=[]
        if (len(aa)>2):
        # prendo solo il primo elemento
            misura=aa[1]['datarow'].split(";")[1]
            valido=aa[1]['datarow'].split(";")[2]
            QueryInsert=Inserisci_in_realtime(IRIS_SCHEMA_NAME,IRIS_TABLE_NAME,\
            row.idsensore,row.nometipologia,id_operatore,data_ricerca,misura,AUTORE)
            try:
                conn.execute(QueryInsert)
                if (eval(DEBUG)):
                    logging.info("+++"+str(row.idsensore)+" "+ data_ricerca+" "+str(misura))
            except:
                        if(eval(DEBUG)):
                            logging.error("Query non riuscita! per "+str(row.idsensore))
        else:
            if (eval(DEBUG)):
                logging.warning("Attenzione: dato di "+h+ " sensore "+str( row.idsensore)+ " ASSENTE nel REM")
    #fine ciclo sensore
'''
QueryDelete='DELETE FROM '+'"'+IRIS_SCHEMA_NAME+'"."'+IRIS_TABLE_NAME+'"' +' WHERE data_e_ora <'+"'"+data_elimina.strftime("%Y-%m-%d %H:%M")+"'"
try:
    conn.execute(QueryDelete)
    if (eval(DEBUG)):
        logging.info("+++pulizia dati eseguita")
except:
    logging.error("ERR: Pulizia dati non riuscita")
'''
print("Recupero terminato per",TIPOLOGIE,"inizio",s,"fine", dt.datetime.now())
vstr=''
logging.info("Recupero terminato per "+ vstr.join(TIPOLOGIE) +" inizio "+s.strftime("%Y-%m-%d %H:%M:%s")+ " fine "+ dt.datetime.now().strftime("%Y-%m-%d %H:%M:%s"))
