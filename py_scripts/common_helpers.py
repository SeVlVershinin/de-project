import datetime
import os
import shutil

STRING_DATE_REPR_FORMAT = '%d%m%Y'
PROCESSED_FILE_FOLDER = 'archive'
PROCESSED_FILE_EXTENSION = '.backup'


def datetime_to_string_repr(dt: datetime.datetime) -> str:
    """
    Преобразует дату в строковое представление, заданное константой STRING_DATE_REPR_FORMAT
    :param dt: дата
    """
    return dt.strftime(STRING_DATE_REPR_FORMAT)


def sql_column_list(columns: list[str], prefix: str = '') -> str:
    """
    Возвращает строку, содержащую перечисление столбцов из columns с префиксом prefix в формате SQL
    """
    return ', '.join([f'{prefix}{x}' for x in columns])


def sql_value_placeholders(columns: list[str]) -> str:
    """
    Возвращает строку, содержащую перечисление %s, по количеству соответствующих столбцам в columns, в формате SQL
    """
    return ', '.join(['%s'] * len(columns))


def move_file_to_processed_folder(filename: str) -> None:
    """
    Перемещает файл filename в папку archive, добавляя к нему расширение, заданное в PROCESSED_FILE_EXTENSION
    """
    processed_filename = os.path.join(PROCESSED_FILE_FOLDER, filename + PROCESSED_FILE_EXTENSION)
    # shutil.copy(filename, processed_filename)
    shutil.move(filename, processed_filename)
