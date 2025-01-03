from py_scripts.logger import logger


def generate_reports(current_date, cursor):
    """
    Выполняет генерацию отчетов для переданной даты
    :param current_date: "текущая" дата, для которой генерируются отчеты
    :param cursor: курсор к БД
    """
    logger.info(f'Процесс построения отчетов запущен для даты {current_date}')
    _generate_report_for_passport_fraud(current_date, cursor)
    _generate_report_for_contract_fraud(current_date, cursor)
    _generate_report_for_two_or_more_cities_operations(current_date, cursor)
    logger.info(f'Процесс построения отчетов завершен для даты {current_date}')


def _generate_report_for_passport_fraud(current_date, cursor):
    query = f"""
        insert into public.sevl_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
        select
            tr.trans_date as event_dt,
            cl.passport_num as passport,
            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio,
            cl.phone as phone,
            'Заблокированный или просроченный паспорт' as event_type,
            %s as report_dt
        from public.sevl_dwh_fact_transactions tr
        join public.sevl_dwh_dim_cards crd
            on crd.card_num = tr.card_num
        join public.sevl_dwh_dim_accounts acc
            on acc.account_num = crd.account_num
        join public.sevl_dwh_dim_clients cl
            on cl.client_id = acc.client
        left join public.sevl_dwh_fact_passport_blacklist blk
            on blk.passport_num = cl.passport_num
        where
            tr.trans_date::date = %s and 
            (coalesce(cl.passport_valid_to, '2100-12-31') < %s or blk.entry_dt is not null);
    """
    dt = current_date.date()
    cursor.execute(query, (dt, dt, dt))


def _generate_report_for_contract_fraud(current_date, cursor):
    query = f"""
        insert into public.sevl_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
        select
            tr.trans_date as event_dt,
            cl.passport_num as passport,
            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio,
            cl.phone as phone,
            'Недействующий договор' as event_type,
            %s as report_dt
        from public.sevl_dwh_fact_transactions tr
        join public.sevl_dwh_dim_cards crd
            on crd.card_num = tr.card_num
        join public.sevl_dwh_dim_accounts acc
            on acc.account_num = crd.account_num
        join public.sevl_dwh_dim_clients cl
            on cl.client_id = acc.client
        where
            tr.trans_date::date = %s and 
            acc.valid_to < %s;
    """
    dt = current_date.date()
    cursor.execute(query, (dt, dt, dt))


def _generate_report_for_two_or_more_cities_operations(current_date, cursor):
    query = f"""
        WITH extended_daily_transactions_data as
                 (SELECT tr.trans_id,
                         array_agg(tr.trans_id) OVER (
                             PARTITION BY clt.client_id
                             ORDER BY tr.trans_date
                             RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
                             ) AS transaction_ids_last_hour,
                         array_agg(trm.terminal_city) OVER (
                             PARTITION BY clt.client_id
                             ORDER BY tr.trans_date
                             RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
                             ) AS transaction_cities_last_hour
                  FROM public.sevl_dwh_fact_transactions tr
                           JOIN public.sevl_dwh_dim_cards crd ON tr.card_num = crd.card_num
                           JOIN public.sevl_dwh_dim_accounts acc ON crd.account_num = acc.account_num
                           JOIN public.sevl_dwh_dim_clients clt ON acc.client = clt.client_id
                           JOIN public.sevl_dwh_dim_terminals trm on tr.terminal = trm.terminal_id
                  WHERE tr.trans_date >= %s::timestamp - INTERVAL '1' HOUR
                    AND tr.trans_date < %s::timestamp + INTERVAL '1' DAY
                  ),
        
             daily_fraud_transaction_ids as
                 (SELECT DISTINCT unnest(MIN(transaction_ids_last_hour)) AS trans_id
                  FROM extended_daily_transactions_data,
                       LATERAL unnest(transaction_cities_last_hour) AS city
                  GROUP BY trans_id
                  having COUNT(DISTINCT city) > 1)
        
        INSERT INTO public.sevl_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
        SELECT
            tr.trans_date AS event_dt,
            cl.passport_num AS passport,
            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic AS fio,
            cl.phone AS phone,
            'Совершение операций в разных городах за короткое время' AS event_type,
            %s AS report_dt
        FROM sevl_dwh_fact_transactions tr
        JOIN sevl_dwh_dim_cards crd
        ON crd.card_num = tr.card_num
        JOIN sevl_dwh_dim_accounts acc
        ON acc.account_num = crd.account_num
        JOIN sevl_dwh_dim_clients cl
        ON cl.client_id = acc.client
        JOIN daily_fraud_transaction_ids fr_tr ON fr_tr.trans_id = tr.trans_id
        WHERE tr.trans_date::date = %s;
    """
    dt = current_date.date()
    cursor.execute(query, (dt, dt, dt, dt))
