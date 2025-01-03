import datetime

from py_scripts.etl_helpers import (
    load_dim_data_from_source_table,
    load_dim_data_from_source_xls,
    load_fact_data_from_source_xls,
    load_fact_data_from_source_txt
)
from py_scripts.logger import logger


def load_data_into_dwh(current_date:datetime.datetime, cursor):
    """
    Загрузка данных измерений и фактов в хранилище данных
    :param current_date: "текущая" дата, для которой выполняется загрузка данных
    :param cursor: курсор к БД
    """
    logger.info(f'ETL-процесс запущен для даты {current_date}')
    # очистка стейдж-таблиц
    _clean_staging_tables(cursor)

    # загрузка данных измерений из БД-источника
    _load_clients(cursor)
    _load_accounts(cursor)
    _load_cards(cursor)

    # загрузка данных измерений из файлов
    _load_terminals(cursor, current_date)

    # загрузка фактов из файлов
    _load_blacklist(cursor, current_date)
    _load_transactions(current_date, cursor)
    logger.info(f'ETL-процесс завершен для даты {current_date}')


def _clean_staging_tables(cursor):
    staging_tables = ['terminals', 'passport_blacklist', 'transactions', 'accounts', 'cards', 'clients']
    query = "\n".join([f"DELETE FROM public.sevl_stg_{table};" for table in staging_tables])
    cursor.execute(query)


def _load_clients(cursor):
    load_dim_data_from_source_table(
        source_table_full_name='info.clients',
        source_table_columns=['client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth',
                              'passport_num', 'passport_valid_to', 'phone'],
        process_source_dataframe_fn=None,
        staging_table_full_name='public.sevl_stg_clients',
        staging_table_columns=['client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth',
                               'passport_num', 'passport_valid_to', 'phone'],
        target_table_full_name='public.sevl_dwh_dim_clients',
        target_table_columns=['client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth',
                              'passport_num', 'passport_valid_to', 'phone'],
        cursor=cursor
    )


def _load_accounts(cursor):
    load_dim_data_from_source_table(
        source_table_full_name='info.accounts',
        source_table_columns=['account', 'valid_to', 'client'],
        process_source_dataframe_fn=None,
        staging_table_full_name='public.sevl_stg_accounts',
        staging_table_columns=['account_num', 'valid_to', 'client'],
        target_table_full_name='public.sevl_dwh_dim_accounts',
        target_table_columns=['account_num', 'valid_to', 'client'],
        cursor=cursor
    )


def _load_cards(cursor):
    load_dim_data_from_source_table(
        source_table_full_name='info.cards',
        source_table_columns=['card_num', 'account'],
        process_source_dataframe_fn=None,
        staging_table_full_name='public.sevl_stg_cards',
        staging_table_columns=['card_num', 'account_num'],
        target_table_full_name='public.sevl_dwh_dim_cards',
        target_table_columns=['card_num', 'account_num'],
        cursor=cursor,
    )


def _load_terminals(cursor, current_date):
    load_dim_data_from_source_xls(
        source_xls_filename='terminals',
        source_xls_sheet_name='terminals',
        current_date=current_date,
        process_source_dataframe_fn=None,
        staging_table_full_name='public.sevl_stg_terminals',
        staging_table_columns=['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'],
        target_table_full_name='public.sevl_dwh_dim_terminals',
        target_table_columns=['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'],
        cursor=cursor
    )


def _load_blacklist(cursor, cur_date):
    def reorder_columns(df):
        return df[['passport', 'date']]

    load_fact_data_from_source_xls(
        source_xls_filename='passport_blacklist',
        source_xls_sheet_name='blacklist',
        current_date=cur_date,
        process_source_dataframe_fn=reorder_columns,
        staging_table_full_name='public.sevl_stg_passport_blacklist',
        staging_table_columns=['passport_num', 'entry_dt'],
        target_table_full_name='public.sevl_dwh_fact_passport_blacklist',
        target_table_columns=['passport_num', 'entry_dt'],
        cursor=cursor
    )


def _load_transactions(cur_date, cursor):
    def replace_decimal_sep_and_add_space_to_card_numbers(df):
        df['amount'] = df['amount'].str.replace(',', '.')
        df['card_num'] = df['card_num'] + ' '
        return df

    load_fact_data_from_source_txt(
        source_txt_filename='transactions',
        source_txt_separator=';',
        current_date=cur_date,
        process_source_dataframe_fn=replace_decimal_sep_and_add_space_to_card_numbers,
        staging_table_full_name='public.sevl_stg_transactions',
        staging_table_columns=['trans_id', 'trans_date', 'amt', 'card_num', 'oper_type', 'oper_result',
                               'terminal'],
        target_table_full_name='public.sevl_dwh_fact_transactions',
        target_table_columns=['trans_id', 'trans_date', 'amt', 'card_num', 'oper_type', 'oper_result',
                              'terminal'],
        cursor=cursor
    )
