# feed IRIS Lampo
Alimentazione di IRIS tramite script python in sostituzione di recupero_RT

# prerequisiti
1. rwmwsgwyd funzionante
2. tabelle di IRIS per il temporeale su postgres
3. pacchetti di python indicati nel codice

# utilizzo
Il recupero viene gestito dal launcher che accetta in ingresso tre parametri
```
./feed_irisLampo  arg1 arg2 arg3
```

_arg1_ minuto al quale esegue il comando

_arg2_ flag per recupero: *R* esegue il recupero, ogni altro carattere non esegue il recupero (es. *N*)

_arg3_ tempo in secondi tra una esecuzione e la successiva (*facoltativo, default 3600*)


# ENV
Nel container sono già esplicitate le variabili d'ambiente di base:

IRIS_USER_ID *postgres*

IRIS_DB_NAME *iris_base*

IRIS_DB_HOST *10.10.0.19*

Devono essere specificate le variabili:

DEBUG *True* scrive tutti gli errori o gli inserimenti dei dati, *False* scrive solo le informazioni essenziali

Da specificare a riga di comando o su file env: 

TIPOLOGIE="ZTD ZWD GE GN" : elenco delle tipologie per cui esegue l'alimentazione/recupero

MINUTES=240 minuti di ritardo rispetto all'orario di lancio per il recupero o l'alimentazione diretta (*vedi note*)

DELTAT=125 tempo di ricerca dati rispetto a ora attuale (CED) - MINUTES

IRIS_USER_PWD password dell'utente IRIS_USER_ID

NAME nome dell'autore dell'inserimento

TEST=N (Y) Serve per testare il recupero e per attivare un log verboso


# esempio
```
docker run -d --rm -v "$PWD":/usr/src/myapp -w /usr/src/myapp --name "test"  --env-file env_docker.sh arpasmr/feed_iris ./launch_feedLampo.sh 15 L 60

comando orig di feedIris:
docker run -d --rm -v "$PWD":/usr/src/myapp -w /usr/src/myapp -e "IRIS_USER_ID=postgres" -e "IRIS_USER_PWD=<password>" -e "IRIS_DB_NAME=iris_base" -e "IRIS_DB_HOST=10.10.0.19" -e "TIPOLOGIE=I PP T UR N RG PA VV DV" -e "DEBUG=True" -e "MINUTES=1440" --name "recupero_all" arpasmr/feed_iris ./launch_feed.sh 8 R 3600
```
# note: uso di MINUTES
L'esecuzione di un'alimentazione ritardata (*MINUTES*>>0) può essere conveniente nel caso di sensori che sono sistematicamente in ritardo (es. sensori RRQA). In questo caso conviene impostare un recupero (*arg2*=R che peschi alcuni *MINUTES* minuti fa (es. *30*) e che venga lanciato ogni 10 minuti (ponendo *arg3* pari a 600).

Per il *recupero* il valore di *MINUTES* deve essere 1440 (per recuperare i dati delle 24h precedenti) mentre *arg1* può essere un valore qualunque.

Nel caso del recupero di 24h il valore di *arg3* (intervallo tra un'esecuzione e la successiva) va posto pari ad almeno 3600 secondi (che è il valore di default e può essere omesso)

L'alimentazione diretta cerca il dato dall'ora solare all'indietro di *MINUTES* minuti.

Il recupero cerca tutti i dati dall'ora UTC all'indietro di *MINUTES* minuti
