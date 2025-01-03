import argparse
import datetime
from dataclasses import dataclass

@dataclass
class Settings:
    host: str
    port: str
    dbname: str
    user: str
    password: str
    processing_dates: list[datetime.datetime]

    def __init__(self):
        args = parser.parse_args()
        self.host = args.host
        self.port = args.port
        self.dbname = args.dbname
        self.user = args.user
        self.password = args.password
        self.processing_dates = args.processing_dates


parser = argparse.ArgumentParser(
    prog='python main.py',
    description='скрипт запуска etl-процесса',
    usage='python main.py [аргументы]',
)

parser.add_argument(
    '--host',
    type=str,
    help='адрес хоста с Postgres',
    metavar='<postgresql host>',
    required=True
)
parser.add_argument(
    '--port',
    type=str,
    help='порт для подключения к Postgres на хосте',
    metavar='<postgres port>',
    required=True
)

parser.add_argument(
    '--dbname',
    type=str,
    help='имя базы данных',
    metavar='<db name>',
    required=True
)

parser.add_argument(
    '--user',
    type=str,
    help='имя пользователя',
    metavar='<user>',
    required=True
)

parser.add_argument(
    '--password',
    type=str,
    help='пароль пользователя',
    metavar='<password>',
    required=True
)


def valid_date(date_str):
    date_str_list = date_str.split(',')
    dates = []
    for date_str in date_str_list:
        try:
            date = datetime.datetime.strptime(date_str, "%Y%m%d")
            dates.append(date)
        except:
            raise argparse.ArgumentTypeError("Даты должна быть указаны в формате YYYYmmdd")
    if len(dates) < 1:
        raise argparse.ArgumentTypeError("Для параметра должны быть указана хотя бы одна дата")
    return dates


parser.add_argument(
    '--processing-dates',
    type=valid_date,
    help='одна или несколько разделенных запятой дат в формате YYYYmmdd, данные за которую(ые) будут обработаны',
    default=[datetime.datetime.now()],
    metavar='<processing dates>',
)
