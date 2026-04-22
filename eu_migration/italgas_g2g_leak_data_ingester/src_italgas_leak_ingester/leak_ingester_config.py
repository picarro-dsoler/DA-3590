import os

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

LOGS_PATH = os.path.abspath(os.path.join(DIR_PATH, '..', 'logs'))
DATABASE_PATH = os.path.abspath(os.path.join(DIR_PATH, '..', 'database/italgas_g2g.db'))

LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'standard',
            'filename': os.path.join(LOGS_PATH, 'leak_data_ingester.log')
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['default'],
            'level': 'WARNING',
            'propagate': False
        },
        'ingest_leak_data': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False
        },
    }
}

italgas_g2g_api_url_dict = {'production': "https://servizi.italgas.it/apiservice/pcubedintegration",
                            'test': "https://servizi-np1.italgas.it/apiservice_test1/pcubedintegration"}

EXPECTED_LEAK_COLUMNS = {
    'numProgressivo',
    'dataInserimento',
    'lisa',
    'codStato',
    'statoFoglietta',
    'codValidazione',
    'statoValidazione',
    'cap',
    'comune',
    'dataArrivoSulCampo',
    'dataLocalizzazione',
    'dataRiparazione',
    'codiceDispersione',
    'indirizzo',
    'aereoInterrato',
    'intervento',
    'indirizzoLisa',
    'indirizzoLocalizzazione',
    'indirizzoRiparazione',
    'accertamentoRiscontrato',
    'descrizioneAsset',
    'sedeTecnicaLocalizzata',
    'xCoord',
    'yCoord',
    'dataUltimaMod',
    'idAzienda'
    }

DATE_COLUMNS = [
    'dataArrivoSulCampo',
    'dataInserimento',
    'dataLocalizzazione',
    'dataRiparazione',
    'dataUltimaMod',
]
