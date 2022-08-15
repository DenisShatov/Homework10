import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Stock, Shop, Sale
import json
import os


login = 'postgres'
password = '********'
adress_srv = 'localhost'
port_srv = '5432'
db_name = 'HW10'

DSN = f'postgresql://{login}:{password}@{adress_srv}:{port_srv}/{db_name}'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine) # создание таблиц, на основе меделей классов

Session = sessionmaker(bind=engine)
session = Session()


# заполнение бд из файла
work_dir = os.getcwd()
name_dir = 'fixtures'
list_files = os.listdir(r"fixtures")

for name_file in list_files:
    with open(os.path.join(work_dir, name_dir, name_file), 'r') as td:
        jdata = json.load(td)

    for record in jdata:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()


# запрос выборки магазинов, продающих целевого издателя
input_data = input('Введите id или наименование издателя:  ')
if input_data.isnumeric():
    for c in session.query(Shop).join(Stock.shop).join(Stock.book).join(Book.publisher) \
            .filter(Publisher.id == input_data):
        print(c)
else:
    for c in session.query(Shop).join(Stock.shop).join(Stock.book).join(Book.publisher) \
            .filter(Publisher.name == input_data):
        print(c)


session.close()
