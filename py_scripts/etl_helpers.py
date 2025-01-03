import datetime

import pandas as pd
from typing import Callable

from .common_helpers import (
    datetime_to_string_repr,
    move_file_to_processed_folder,
    sql_column_list,
    sql_value_placeholders
)

UPDATE_DT_FIELD_NAME = 'update_dt'
CREATE_DT_FIELD_NAME = 'create_dt'
METADATA_TABLE_FULL_NAME = 'public.sevl_meta_info'


def load_dim_data_from_source_xls(
        source_xls_filename: str,
        source_xls_sheet_name: str,
        current_date: datetime.datetime,
        process_source_dataframe_fn: Callable[[pd.DataFrame], pd.DataFrame] | None,
        staging_table_full_name: str,
        staging_table_columns: list[str],
        target_table_full_name: str,
        target_table_columns: list[str],
        cursor
):
    """
    Выполняет загрузку данных из исходного xls-файла с ежедневной полной выгрузкой значений измерения в соответствующую
    таблицу в DWH. Имя файла на диске должно иметь формат source_xls_filename_DDMMYYYY.xlsx, где DDMMYYYY - дата,
    за которую загружаются данные.

    Данные в файле не должны содержать колонок с датами создания и изменения строк. При загрузке данных в
    качестве этих дат берется current_date, в результате чего строки, появившиеся впервые, создаются в DWH с этой датой,
    а строки, которые были в предыдущих выгрузках, обновляются этой датой.
    Для хранения дат создания и обновления записей стейдж-таблица и таблица в DWH должны использовать поля с именами
    CREATE_DT_FIELD_NAME и UPDATE_DT_FIELD_NAME

    В случае, если current_date меньше или равна дате последнего обновления данных (т.е. ранее в DWH уже были загружены
    более свежие данные), то функция не выполняет обработку xls-файла.

    :param source_xls_filename: имя xls-файла с данными измерения (без расширения и даты)
    :param source_xls_sheet_name: имя листа в xls-файле
    :param current_date: дата, для которой загружаются данные (в случае реальной ежедневной загрузки - текущая дата,
    а в случае загрузки данных за требуемые периоды - дата, за которую загружаются данные)
    :param process_source_dataframe_fn: функция дополнительной обработки данных перед загрузкой в стейдж-таблицу,
    которую можно использовать для доп.очистки данных или смены порядка столбцов и т.д. В случае, если обработка не
    нужна - передается None
    :param staging_table_full_name: полное имя стейдж-таблицы, включая имя схемы
    :param staging_table_columns: все столбцы стейдж-таблицы, за исключением столбцов с датой создания и обновления.
    Первым в списке должен быть столбец первичного ключа.
    :param target_table_full_name: полное имя таблицы в DWH, включая имя схемы
    :param target_table_columns: все столбцы таблицы в DWH, за исключением столбцов с датой создания и обновления.
    Первым в списке должен быть столбец первичного ключа, а порядок следования столбцов должен совпадать с порядком
    следования столбцов в стейдж-таблице
    :param cursor: курсор для доступа к БД
    """
    # из таблицы с метаданными получаем дату последнего обновления данных
    max_update_timestamp = _get_max_update_timestamp(staging_table_full_name, cursor)

    # если файл содержит данные, предшествующие или равные дате последнего обновления, то мы их уже не обрабатываем
    if current_date <= max_update_timestamp:
        return

    # получаем все данные из файла; так как они не в формате SCD1, то загружаем их целиком,
    # а датой их создания и обновления считаем текущую дату (такой подход приводит к тому,
    # что данные, пришедшие к нам впервые, будут "созданы" текущей датой, а те, которые уже есть
    # у нас в DWH - этой же датой "обновлены", что логично)
    full_xls_filename = f"{source_xls_filename}_{datetime_to_string_repr(current_date)}.xlsx"
    df = _select_dim_changes_from_source_xls(full_xls_filename, source_xls_sheet_name, current_date)

    # выполняем дополнительную обработку датафрейма, если она указана среди аргументов
    if process_source_dataframe_fn:
        df = process_source_dataframe_fn(df)

    # переносим файл в каталог archive с одновременным добавлением к нему расширения .backup
    move_file_to_processed_folder(full_xls_filename)

    # далее вставляем полученные данные в стейдж-таблицу, используя ту же функцию, что и для загрузки из БД
    _insert_dim_changes_into_staging_table(df, staging_table_columns, staging_table_full_name, cursor)
    # так как файл содержит полный срез данных и мы загрузили его в стейдж-таблицу целиком, то и перечень ИД можно
    # получить из нее
    existing_source_ids = _select_existing_ids_from_source_table(staging_table_full_name, staging_table_columns, cursor)

    # переносим полученные данные из стейдж-таблицы в DWH используя общую для измерений логику переноса
    _load_dim_changes_into_target_table(staging_table_full_name, staging_table_columns, target_table_full_name,
                                        target_table_columns, existing_source_ids, cursor)

    # записываем в таблицу с метаданными дату последнего обновления данных
    _set_max_update_timestamp_from_staging_table_data(staging_table_full_name, max_update_timestamp, cursor)


def load_dim_data_from_source_table(
        source_table_full_name,
        source_table_columns,
        process_source_dataframe_fn: Callable[[pd.DataFrame], pd.DataFrame] | None,
        staging_table_full_name,
        staging_table_columns,
        target_table_full_name,
        target_table_columns,
        cursor,
):
    """
    Выполняет загрузку данных из таблицы-источника со значениями измерений в соответствующую таблицу в DWH. Для
    хранения дат создания и обновления записей таблица-источник, стейдж-таблица и таблица в DWH должны использовать
    поля с именами CREATE_DT_FIELD_NAME и UPDATE_DT_FIELD_NAME

    :param source_table_full_name: полное имя таблицы-источника, включая имя схемы
    :param source_table_columns: все столбцы таблицы-источника, за исключением столбцов с датой создания и обновления.
    Первым в списке должен быть столбец первичного ключа
    :param process_source_dataframe_fn: функция дополнительной обработки данных перед загрузкой в стейдж-таблицу,
    которую можно использовать для доп.очистки данных или смены порядка столбцов и т.д. В случае, если обработка не
    нужна - передается None
    :param staging_table_full_name: полное имя стейдж-таблицы, включая имя схемы
    :param staging_table_columns: все столбцы стейдж-таблицы, за исключением столбцов с датой создания и обновления.
    Первым в списке должен быть столбец первичного ключа, а порядок следования столбцов должен совпадать с порядком
    следования столбцов в таблице-источнике
    :param target_table_full_name: полное имя таблицы в DWH, включая имя схемы
    :param target_table_columns: все столбцы таблицы в DWH, за исключением столбцов с датой создания и обновления.
    Первым в списке должен быть столбец первичного ключа, а порядок следования столбцов должен совпадать с порядком
    следования столбцов в стейдж-таблице
    :param cursor: курсор для доступа к БД
    :return:
    """
    # из таблицы с метаданными получаем дату последнего обновления данных
    max_update_timestamp = _get_max_update_timestamp(staging_table_full_name, cursor)

    # загружаем изменившиеся с момента последнего обновления данные из таблицы-источника
    df = _select_dim_changes_from_source_table(max_update_timestamp, source_table_columns, source_table_full_name,
                                               cursor)

    # выполняем дополнительную обработку датафрейма, если она указана среди аргументов
    if process_source_dataframe_fn:
        df = process_source_dataframe_fn(df)

    # получаем список идентификаторов всех строк, которые существуют в источнике для последующего удаления тех
    # строк в хранилище, которые были удалены в источнике)
    existing_source_ids = _select_existing_ids_from_source_table(source_table_full_name, source_table_columns, cursor)

    # сохраняем полученные данные в стейдж-таблице
    _insert_dim_changes_into_staging_table(df, staging_table_columns, staging_table_full_name, cursor)

    # загружаем данные из стейдж-таблицы в таблицу в DWH
    _load_dim_changes_into_target_table(staging_table_full_name, staging_table_columns, target_table_full_name,
                                        target_table_columns, existing_source_ids, cursor)

    # записываем в таблицу с метаданными дату последнего обновления данных
    _set_max_update_timestamp_from_staging_table_data(staging_table_full_name, max_update_timestamp, cursor)


def load_fact_data_from_source_xls(
        source_xls_filename: str,
        source_xls_sheet_name: str,
        current_date: datetime.datetime,
        process_source_dataframe_fn: Callable[[pd.DataFrame], pd.DataFrame],
        staging_table_full_name: str,
        staging_table_columns: list[str],
        target_table_full_name: str,
        target_table_columns: list[str],
        cursor,
        update_existing_facts: bool = False
):
    # формируем имя файла и функцию его загрузки в DataFrame
    full_xls_filename = f"{source_xls_filename}_{datetime_to_string_repr(current_date)}.xlsx"

    def load_file_data():
        return _select_fact_changes_from_source_xls(full_xls_filename, source_xls_sheet_name)

    # вызываем обобщенную функцию загрузку фактов в DWH
    _load_fact_data_from_source_file(
        load_file_data,
        current_date,
        process_source_dataframe_fn,
        staging_table_full_name,
        staging_table_columns,
        target_table_full_name,
        target_table_columns,
        cursor,
        update_existing_facts=update_existing_facts
    )
    # переносим файл в каталог archive с одновременным добавлением к нему расширения .backup
    move_file_to_processed_folder(full_xls_filename)


def load_fact_data_from_source_txt(
        source_txt_filename: str,
        source_txt_separator: str,
        current_date: datetime.datetime,
        process_source_dataframe_fn: Callable[[pd.DataFrame], pd.DataFrame],
        staging_table_full_name: str,
        staging_table_columns: list[str],
        target_table_full_name: str,
        target_table_columns: list[str],
        cursor,
        update_existing_facts: bool = False
):
    # формируем имя файла и функцию его загрузки в DataFrame
    full_txt_filename = f"{source_txt_filename}_{datetime_to_string_repr(current_date)}.txt"

    def load_file_data():
        return _select_fact_changes_from_source_txt(full_txt_filename, source_txt_separator)

    # вызываем обобщенную функцию загрузку фактов в DWH
    _load_fact_data_from_source_file(
        load_file_data,
        current_date,
        process_source_dataframe_fn,
        staging_table_full_name,
        staging_table_columns,
        target_table_full_name,
        target_table_columns,
        cursor,
        update_existing_facts=update_existing_facts
    )
    # переносим файл в каталог archive с одновременным добавлением к нему расширения .backup
    move_file_to_processed_folder(full_txt_filename)


def _load_fact_data_from_source_file(
        source_file_loader_fn: Callable[[], pd.DataFrame],
        current_date: datetime.datetime,
        process_source_dataframe_fn: Callable[[pd.DataFrame], pd.DataFrame],
        staging_table_full_name: str,
        staging_table_columns: list[str],
        target_table_full_name: str,
        target_table_columns: list[str],
        cursor,
        update_existing_facts: bool = False
):
    max_update_timestamp = _get_max_update_timestamp(staging_table_full_name, cursor)
    if current_date <= max_update_timestamp:
        return

    # загружаем данные из файла
    df = source_file_loader_fn()

    # выполняем дополнительную обработку датафрейма, если она указана среди аргументов
    if process_source_dataframe_fn:
        df = process_source_dataframe_fn(df)

    # сохраняем данные в стейдж-таблицу
    _insert_fact_changes_into_staging_table(df, staging_table_columns, staging_table_full_name, cursor)

    # переносим полученные данные из стейдж-таблицы в DWH
    _load_fact_changes_into_target_table(staging_table_full_name, staging_table_columns, target_table_full_name,
                                         target_table_columns, cursor, update_existing_facts=update_existing_facts)

    # записываем в таблицу с метаданными current_date в качестве даты последнего обновления данных
    _set_precalculated_max_update_timestamp(staging_table_full_name, current_date, cursor)


def _get_max_update_timestamp(table_name, cursor):
    cursor.execute(f"select max_update_dt from {METADATA_TABLE_FULL_NAME} where table_name='{table_name}'")
    return cursor.fetchone()[0]


def _set_max_update_timestamp_from_staging_table_data(table_name, prev_timestamp, cursor):
    cursor.execute(f"update {METADATA_TABLE_FULL_NAME} "
                   f"set max_update_dt = coalesce( (select max( {UPDATE_DT_FIELD_NAME} ) from {table_name} ), %s )"
                   f"where table_name='{table_name}';", (prev_timestamp,))


def _set_precalculated_max_update_timestamp(table_name, max_update_timestamp, cursor):
    cursor.execute(f"update {METADATA_TABLE_FULL_NAME} "
                   f"set max_update_dt = %s "
                   f"where table_name='{table_name}';", (max_update_timestamp,))


def _select_dim_changes_from_source_xls(filename: str, sheet_name: str,
                                        current_date: datetime.datetime) -> pd.DataFrame:
    df = _load_xls(filename, sheet_name)
    df[CREATE_DT_FIELD_NAME] = current_date
    df[UPDATE_DT_FIELD_NAME] = current_date
    return df


def _select_fact_changes_from_source_xls(filename: str, sheet_name: str) -> pd.DataFrame:
    return _load_xls(filename, sheet_name)


def _select_fact_changes_from_source_txt(filename: str, separator: str) -> pd.DataFrame:
    return _load_txt(filename, separator)


def _load_xls(filename: str, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(
        filename,
        sheet_name=sheet_name,
        header=0,
        index_col=None)
    return df


def _load_txt(filename: str, separator: str) -> pd.DataFrame:
    df = pd.read_csv(
        filename,
        header=0,
        sep=separator,
        index_col=None)
    return df


def _select_dim_changes_from_source_table(max_update_timestamp, source_table_columns, source_table_full_name, cursor):
    cursor.execute(
        f"select "
        f"{sql_column_list(source_table_columns)}, "
        f"{CREATE_DT_FIELD_NAME}, "
        f"{UPDATE_DT_FIELD_NAME} "
        f"from {source_table_full_name} "
        f"where coalesce({UPDATE_DT_FIELD_NAME}, {CREATE_DT_FIELD_NAME}) > %s",
        (max_update_timestamp,))
    col_names = [x[0] for x in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=col_names)
    return df


def _select_existing_ids_from_source_table(source_table_full_name, source_table_columns, cursor):
    source_table_pk = source_table_columns[0]
    cursor.execute(f"select {source_table_pk} from {source_table_full_name};")
    existing_row_ids = tuple((r[0] for r in cursor.fetchall()))
    return existing_row_ids


def _insert_dim_changes_into_staging_table(source_dataframe, staging_table_columns, staging_table_full_name, cursor):
    cursor.executemany(
        f"INSERT INTO {staging_table_full_name}("
        f"{sql_column_list(staging_table_columns)}, "
        f"{CREATE_DT_FIELD_NAME}, "
        f"{UPDATE_DT_FIELD_NAME}) "
        f"VALUES ({sql_value_placeholders(staging_table_columns)}, %s, %s)",
        source_dataframe.values.tolist())

    cursor.execute(f"update {staging_table_full_name} "
                   f"set {UPDATE_DT_FIELD_NAME} = coalesce({UPDATE_DT_FIELD_NAME}, {CREATE_DT_FIELD_NAME})")


def _insert_fact_changes_into_staging_table(source_dataframe, staging_table_columns, staging_table_full_name, cursor):
    cursor.executemany(
        f"INSERT INTO {staging_table_full_name}("
        f"{sql_column_list(staging_table_columns)}) "
        f"VALUES ({sql_value_placeholders(staging_table_columns)})",
        source_dataframe.values.tolist())


def _load_dim_changes_into_target_table(staging_table_full_name, staging_table_columns, target_table_full_name,
                                        target_table_columns, existing_source_ids, cursor):
    staging_table_pk = staging_table_columns[0]
    target_table_pk = target_table_columns[0]

    # загружаем в DWH-таблицу новые строки из таблицы-источника
    query = f"""
            insert into {target_table_full_name}(
                {sql_column_list(target_table_columns)}, 
                {CREATE_DT_FIELD_NAME}, 
                {UPDATE_DT_FIELD_NAME})
            select
                {sql_column_list(staging_table_columns, 'stg.')},
                stg.{UPDATE_DT_FIELD_NAME},
                stg.{CREATE_DT_FIELD_NAME}
            from {staging_table_full_name} stg
            left join {target_table_full_name} tgt
            on stg.{staging_table_pk} = tgt.{target_table_pk}
            where tgt.{staging_table_pk} is null;
        """
    cursor.execute(query)

    # обновляем в DWH-таблице измененные в таблице-источнике строки
    query = f"""
            update {target_table_full_name}
            set
                {", ".join(f"{x} = tmp.{y}" for x, y in zip(target_table_columns[1:], staging_table_columns[1:]))},
                {UPDATE_DT_FIELD_NAME} = tmp.{UPDATE_DT_FIELD_NAME}
            from (
                select
                    {sql_column_list(staging_table_columns, 'stg.')},
                    stg.{UPDATE_DT_FIELD_NAME}
                from {staging_table_full_name} stg
                inner join {target_table_full_name} tgt
                on stg.{staging_table_pk} = tgt.{target_table_pk}
                where 
                    {" or ".join(f"(stg.{x} <> tgt.{y}"
                                 f" or (stg.{x} is null and tgt.{y} is not null)"
                                 f" or (stg.{x} is not null and tgt.{y} is null))"
                                 for x, y in zip(staging_table_columns[1:], target_table_columns[1:])
                                 )}
            ) tmp
            where {target_table_full_name}.{target_table_pk} = tmp.{staging_table_pk};
            """
    cursor.execute(query)

    # удаляем в DWH-таблице строк, которых больше нет в таблице-источнике.
    cursor.execute(f"delete from {target_table_full_name} where {target_table_pk} not in %s",
                   (existing_source_ids,))


def _load_fact_changes_into_target_table(staging_table_full_name, staging_table_columns, target_table_full_name,
                                         target_table_columns, cursor, update_existing_facts=False):
    staging_table_pk = staging_table_columns[0]
    target_table_pk = target_table_columns[0]

    value_column_pairs = zip(target_table_columns[1:], staging_table_columns[1:])
    query = f"""
            merge into {target_table_full_name} as tgt
            using {staging_table_full_name} as src
            on tgt.{target_table_pk} = src.{staging_table_pk}
            when matched then
                {", ".join(f"{x} = tgt.{y}" for x, y in value_column_pairs) if update_existing_facts else "do nothing"}
            when not matched then
                insert ({", ".join(target_table_columns)})
                values ({", ".join(f"src.{x}" for x in staging_table_columns)});
    """
    cursor.execute(query)
