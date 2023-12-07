import pandas as pd
from sqlalchemy import create_engine
import glob
import csv
from typing import NewType  # nie wiem co mam mi dać ta bibloteka

#typing rozumiem to po prostu pisane metod zmiennych w bardziej przejrzysty sposób
#trochę mi podpowiedział ten filmik https://www.youtube.com/watch?v=QORvB-_mbZ0



def count_number_of_rows(plik_csv:str)->str:
    with open(plik_csv, 'r', encoding='utf-8') as csvfile: #encoding ="utf-8", add this phrase allow avoid problem with "bajts"
        reader = csv.reader(csvfile)
        numberRows = sum(1 for row in reader) 
    return numberRows


engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
for file in glob.glob("*.csv"):
    df = pd.read_csv(file)
    slownik={}
    df1=df[['id','host_id','host_url','host_acceptance_rate','number_of_reviews']]
    df2=df[['id','latitude','longitude','price']]
    l=list()
    l.append(df1)
    l.append(df2)
    slownik['host_informations']=df1
    slownik['informations_about_accomodations']=df2
    liczba_wierszy = count_number_of_rows(file)
    print(f'Liczba wierszy w pliku CSV: {liczba_wierszy}')
    chunksize = 100000  
    for keys, chunk in slownik.items():
        chunk.to_sql(keys, engine, index=False, if_exists='replace', method='multi')
    print(f"Tabela {file[:-4]} została zapisana w bazie danych.")

engine.dispose()

