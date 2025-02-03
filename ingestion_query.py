from sqlalchemy import create_engine
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

DB_HOST = config["postgresql"]["host"]
DB_NAME = config["postgresql"]["database"]
DB_USER = config["postgresql"]["user"]
DB_PASSWORD = config["postgresql"]["password"]
DB_PORT = config["postgresql"]["port"]


db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(db_url)

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
    print(df)


except Exception as e:
    print("Erro: ",e)