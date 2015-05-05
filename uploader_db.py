import csv
import os

from peewee import TextField
from peewee import PostgresqlDatabase
from peewee import Model


def get_db():
    host = os.getenv("DB_HOST", '127.0.01')
    user = os.getenv("DB_USER", 'root')
    password = os.getenv("DB_PASS", '')
    dbname = os.getenv("DB_NAME", 'phoenix')
    return PostgresqlDatabase(dbname, **{'host': host, 'user': user, 'password': password})


field_list = [("EventID", {"unique": True}),
              ("Date", {"index": True}),
              "Year",
              "Month",
              "Day",
              "SourceActorFull",
              ("SourceActorEntity", {"index": True}),
              ("SourceActorRole", {"index": True}),
              "SourceActorAttribute",
              "TargetActorFull",
              "TargetActorEntity",
              ("TargetActorRole", {"index": True}),
              "TargetActorAttribute",
              ("EventCode", {"index": True}),
              "EventRootCode",
              "QuadClass",
              "GoldsteinScore",
              "Issues",
              "ActionLat",
              "ActionLong",
              "LocationName",
              "GeoCountryName",
              ("GeoStateName", {"index": True}),
              "SentenceID",
              "URLs",
              ("NewsSources", {"index": True}),
              ]
_field_list_clean = []


class BaseModel(Model):
    class Meta:
        database = get_db()


class Events(BaseModel):
    pass


def _init_model():
    global _field_list_clean
    global Events
    for field in field_list:
        if isinstance(field, tuple):
            params = field[1]
            field = field[0]
        else:
            params = {}
        params['null'] = True
        my_field = TextField(**params)
        my_field.add_to_class(Events, field)
        _field_list_clean.append(field)


_init_model()


def parse_row(row):
    row = [x if x else None for x in row]
    return dict(zip(_field_list_clean, row))


def process_rows(rows):
    data = map(parse_row, rows)
    Events.insert_many(data).execute()


def create_tables_if_dne():
    db = get_db()
    for table in [Events]:
        try:
            db.create_table(table)
        except Exception as e:
            if "already exists" not in e.message:
                raise


def main(datestr, server_info, file_info):
    create_tables_if_dne()
    file_name = '{}{}.txt'.format(file_info, datestr)
    with open(file_name, 'r') as temp_file:
        process_rows(csv.reader(temp_file, delimiter='\t'))


