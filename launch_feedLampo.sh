#!/bin/bash
# script di lancio dell'alimentazione di iris tramite pyhton
#
# il primo argomento è il minuto in cui eseguire il comando ogni 10 minuti
# il secondo argomento è in caso di recupero: mettere comunque una lettera diversa da 'R'
# in caso di recupero esegue lo script e POI si ferma per un tempo determinato dal terzo parametro
nomescript=${0##*/}


if [ $3 -ge 0 ]; then
   dormi=$3
else
   dormi=3600
fi

MINSTART=$1
min_start_10=$[ 10#$MINSTART % 10]
#echo "Start script"

while [ 1 ]
do
   data_corrente=$[ 10#$(date +"%M") ]

   time_10m=$[ 10#$(date +"%M") % 10 ]
   if [ $time_10m -eq 0 ]; then echo "Countdown: $data_corrente/$MINSTART"; fi


   if [ $data_corrente == $MINSTART ] && [ $2 != "R" ]; then
      #logger -is -p user.notice "Logger: $nomescript: eseguo alimentazione al minuto $1 per $TIPOLOGIE"
      echo "Minuto: $data_corrente Eseguo alimentazione diretta per $TIPOLOGIE"
      python3 feed_irisLampo.py
      if  [ $TEST == "Y" ]; then
          echo "Fine Test"
          break
      fi	  
   else
       if [ $2 == "R" ] ;then
           echo "Bash: Eeguo recupero per $TIPOLOGIE"
           logger -is -p user.notice "$nomescript: eseguo recupero per $TIPOLOGIE"         
           python3 feed_iris_recuperoLampo.py
           if  [ $TEST == "Y" ]; then
               echo "Fine Test"
               break
           else 
               sleep $dormi
           fi
       fi
   fi
   sleep 60 
done

echo "Fine $0"
sleep 30
