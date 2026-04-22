#! /bin/bash

# save my current directory
MY_CWD=$(pwd)

# creating database directory
mkdir database

# change to database directory
cd database || exit

# create a new database and the Leaks table
# note that leakId is the year of dataInserimento in UTC prepended to numProgressivo
# and all dates in table are UTC epoch seconds
sqlite3 italgas_g2g.db "create table Leaks (
leakId INTEGER PRIMARY KEY,
numProgressivo INTEGER,
lisa TEXT,
aereoInterrato TEXT,
codiceDispersione TEXT,
codStato TEXT,
xCoord REAL,
yCoord REAL,
statoFoglietta TEXT,
codValidazione TEXT,
statoValidazione TEXT,
intervento TEXT,
dataInserimento INTEGER,
dataArrivoSulCampo INTEGER,
dataLocalizzazione INTEGER,
dataRiparazione INTEGER,
cap TEXT,
comune TEXT,
indirizzo TEXT,
indirizzoLisa TEXT,
indirizzoLocalizzazione TEXT,
indirizzoRiparazione TEXT,
accertamentoRiscontrato TEXT,
descrizioneAsset TEXT,
sedeTecnicaLocalizzata TEXT,
dataUltimaMod INTEGER,
picarroLastUpdated INTEGER
);"

# change directory back to the original
cd $MY_CWD || exit

# create cronjob to run every 8 hours (or 3x/day)
crontab -l | {
  /bin/cat
  /bin/echo "45 3,11,19 * * * . /home/ubuntu/anaconda3/envs/picarro3/etc/conda/activate.d/italgas-leak-ingester-activate.sh ; /home/ubuntu/anaconda3/envs/picarro3/bin/python $MY_CWD/src_italgas_leak_ingester/main.py"
} | crontab -

# clean exit
exit
