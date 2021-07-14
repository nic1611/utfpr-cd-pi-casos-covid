import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DATABASE = 'grupo_z'
USER = 'postgres'
PASSWORD = 'admin'
PORT = '5432'
FILE = 'C:\\Users\\bfaam\\Downloads\\utfpr-lpcd\\data\\covid19_casos_brasil.csv'

class Carregador:

    def __init__(self, dbname='postgres', user='postgres', password='Postgres2019!', statement_tables=None, port='5432'):
        self.user = user
        self.password = password
        self.statement_tables = statement_tables
        self.dbname = dbname
        self.port = port
        
    def set_connection(self, dbname='postgres'):
        #self.conn = psycopg2.connect("dbname={} user={} password={}".format(dbname, self.user, self.password))
        self.conn = psycopg2.connect("user={} password={} port={}".format(self.user, self.password, self.port))
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def load_table(self, query, tuples):
        self.set_connection()
        try:
            self.cursor.executemany(query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            self.close_connection()
        print('OK. Tabelas carregadas com sucesso.')
        self.close_connection()

df = pd.read_csv(FILE)

dfTrab = df[df['city'].isin(['Curitiba', 'Rio de Janeiro', 'Fortaleza', 'Maceió', 'Recife'])]
dfTrab['idcasos_covid'] = dfTrab.index
dfTrab['casos_covid_idcasos_covid'] = dfTrab['idcasos_covid']

casos_covid = dfTrab[['idcasos_covid', 'date', 'epidemiological_week', 'last_available_date', 'is_last', 'is_repeated', 'last_available_confirmed', 'last_available_confirmed_per_100k_inhabitants', 'last_available_death_rate', 'last_available_deaths', 'new_confirmed', 'new_deaths']]
casos_covid = casos_covid.rename(columns={'last_available_date': 'casos_covidcol'})

localizacao = dfTrab[['city', 'city_ibge_code', 'estimated_population_2019', 'order_for_place', 'place_type', 'state', 'casos_covid_idcasos_covid']]

casos_covid = casos_covid.astype({
    'last_available_death_rate': 'str',
    'last_available_deaths': 'str',
    'is_last':'int64',
    'is_repeated':'int64'
    })

carregador = Carregador(dbname=DATABASE, user=USER, password=PASSWORD, port=PORT)


tuples = [tuple(x) for x in casos_covid.to_numpy()]
cols = ','.join(list(casos_covid.columns))
query  = "INSERT INTO  %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % ('casos_covid', cols)
carregador.load_table(query, tuples)


tuples = [tuple(x) for x in localizacao.to_numpy()]
cols = ','.join(list(localizacao.columns))
query  = "INSERT INTO  %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % ('localizacao', cols)
carregador.load_table(query, tuples)

