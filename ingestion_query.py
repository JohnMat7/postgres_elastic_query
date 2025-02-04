from sqlalchemy import create_engine
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

#Postgres Conection
DB_HOST = config["postgresql"]["host"]
DB_NAME = config["postgresql"]["database"]
DB_USER = config["postgresql"]["user"]
DB_PASSWORD = config["postgresql"]["password"]
DB_PORT = config["postgresql"]["port"]


db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(db_url)

#ElasticConection
ES_HOST = config["elasticsearch"]["host"]
ES_PORT = config["elasticsearch"]["port"]


def postgres_pulled_count_csv():
    query = """
    SELECT DATE("batch_startTime") AS pulled_date, 
        source, 
        SUM(docs_count) AS total_docs 
    FROM data_migration_db."data_migration_new_keyword_1_Nov_2024" 
    WHERE "batch_startTime" BETWEEN '2025-01-25' AND '2025-02-10' 
    GROUP BY pulled_date, source 
    ORDER BY pulled_date , source
    """

    try:
        df = pd.read_sql(query,engine)
        df.to_csv("Raw.csv")
    #    df_pivot = df.pivot(index="source" , columns="pulled_date" , values="total_docs")
    #    df_pivot.rename(columns=lambda col:f"{col} Pulled Count",inplace=True)
    #    print(df_pivot)
    #    df_pivot.to_csv("Total_Pulled_Count.csv")


    except Exception as e:
        print("Erro: ",e)

def crawled_relevant_csv():
    query = ""