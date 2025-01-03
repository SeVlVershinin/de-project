-- DIMENSIONS
CREATE TABLE public.sevl_dwh_dim_clients
(
    client_id         varchar(10) NOT NULL PRIMARY KEY,
    last_name         varchar(20) NOT NULL,
    first_name        varchar(20) NOT NULL,
    patronymic        varchar(20) NOT NULL,
    date_of_birth     date        NOT NULL,
    passport_num      varchar(15) NOT NULL,
    passport_valid_to date        NULL,
    phone             varchar(16) NOT NULL,
    create_dt         timestamp   NOT NULL,
    update_dt         timestamp   NULL
);

CREATE TABLE public.sevl_dwh_dim_accounts
(
    account_num varchar(20) NOT NULL PRIMARY KEY,
    valid_to    date        NOT NULL,
    client      varchar(10) NOT NULL,
    create_dt   timestamp   NOT NULL,
    update_dt   timestamp   NULL
    ,CONSTRAINT accounts_client_fk FOREIGN KEY (client) REFERENCES public.sevl_dwh_dim_clients (client_id)
);

CREATE TABLE public.sevl_dwh_dim_cards
(
    card_num    varchar(20) NOT NULL PRIMARY KEY,
    account_num varchar(20) NOT NULL,
    create_dt   timestamp   NOT NULL,
    update_dt   timestamp   NULL,
    CONSTRAINT cards_account_fk FOREIGN KEY (account_num) REFERENCES public.sevl_dwh_dim_accounts (account_num)
);

CREATE TABLE public.sevl_dwh_dim_terminals
(
    terminal_id      varchar(6)  NOT NULL PRIMARY KEY,
    terminal_type    varchar(3)  NOT NULL,
    terminal_city    varchar(25) NOT NULL,
    terminal_address varchar(60) NOT NULL,
    create_dt        timestamp   NOT NULL,
    update_dt        timestamp   NULL
);


-- FACTS
CREATE TABLE public.sevl_dwh_fact_transactions
(
    trans_id    varchar(11) NOT NULL PRIMARY KEY,
    trans_date  timestamp   NOT NULL,
    card_num    varchar(20) NOT NULL,
    oper_type   varchar(10) NOT NULL,
    amt         numeric     NOT NULL,
    oper_result varchar(10) NOT NULL,
    terminal    varchar(6)  NOT NULL,
    CONSTRAINT transactions_card_fk FOREIGN KEY (card_num) REFERENCES public.sevl_dwh_dim_cards (card_num),
    CONSTRAINT transactions_terminal_fk FOREIGN KEY (terminal) REFERENCES public.sevl_dwh_dim_terminals (terminal_id)
);

CREATE TABLE public.sevl_dwh_fact_passport_blacklist
(
    passport_num varchar(15) NOT NULL PRIMARY KEY,
    entry_dt     date        NOT NULL
);


-- REPORTS
CREATE TABLE public.sevl_rep_fraud
(
    event_dt   timestamp    NOT NULL,
    passport   varchar(15)  NOT NULL,
    fio        varchar(70)  NOT NULL,
    phone      varchar(16)  NOT NULL,
    event_type varchar(100) NOT NULL,
    report_dt  date         NOT NULL
);

--- META INFO
CREATE TABLE public.sevl_meta_info
(
    table_name    varchar(50)  NULL,
    max_update_dt timestamp(0) NULL
);

insert into public.sevl_meta_info( table_name, max_update_dt )
values('public.sevl_stg_terminals', to_timestamp('1800-01-01','YYYY-MM-DD') )
,('public.sevl_stg_accounts', to_timestamp('1800-01-01','YYYY-MM-DD') )
,('public.sevl_stg_cards', to_timestamp('1800-01-01','YYYY-MM-DD') )
,('public.sevl_stg_clients', to_timestamp('1800-01-01','YYYY-MM-DD') )
,('public.sevl_stg_transactions', to_timestamp('1800-01-01','YYYY-MM-DD') )
,('public.sevl_stg_passport_blacklist', to_timestamp('1800-01-01','YYYY-MM-DD') );



-- STAGING
CREATE TABLE public.sevl_stg_transactions
(
    trans_id    varchar(11) NOT NULL PRIMARY KEY,
    trans_date  timestamp   NOT NULL,
    card_num    varchar(20) NOT NULL,
    oper_type   varchar(10) NOT NULL,
    amt         numeric     NOT NULL,
    oper_result varchar(10) NOT NULL,
    terminal    varchar(6)  NOT NULL
);

CREATE TABLE public.sevl_stg_terminals
(
    terminal_id      varchar(6)  NOT NULL PRIMARY KEY,
    terminal_type    varchar(3)  NOT NULL,
    terminal_city    varchar(25) NOT NULL,
    terminal_address varchar(60) NOT NULL,
    create_dt        timestamp   NOT NULL,
    update_dt        timestamp   NULL
);

CREATE TABLE public.sevl_stg_passport_blacklist
(
    passport_num varchar(15) NOT NULL PRIMARY KEY,
    entry_dt     date        NOT NULL
);


CREATE TABLE public.sevl_stg_clients
(
    client_id         varchar(10) NOT NULL PRIMARY KEY,
    last_name         varchar(20) NOT NULL,
    first_name        varchar(20) NOT NULL,
    patronymic        varchar(20) NOT NULL,
    date_of_birth     date        NOT NULL,
    passport_num      varchar(15) NOT NULL,
    passport_valid_to date        NULL,
    phone             varchar(16) NOT NULL,
    create_dt         timestamp   NOT NULL,
    update_dt         timestamp   NULL
);

CREATE TABLE public.sevl_stg_accounts
(
    account_num varchar(20) NOT NULL PRIMARY KEY,
    valid_to    date        NOT NULL,
    client      varchar(10) NOT NULL,
    create_dt   timestamp   NOT NULL,
    update_dt   timestamp   NULL
);

CREATE TABLE public.sevl_stg_cards
(
    card_num    varchar(20) NOT NULL PRIMARY KEY,
    account_num varchar(20) NOT NULL,
    create_dt   timestamp   NOT NULL,
    update_dt   timestamp   NULL
);


--drop table public.sevl_dwh_fact_transactions;
--drop table public.sevl_dwh_fact_passport_blacklist;

--drop table public.sevl_dwh_dim_terminals;
--drop table public.sevl_dwh_dim_cards;
--drop table public.sevl_dwh_dim_accounts;
--drop table public.sevl_dwh_dim_clients;

--drop table public.sevl_rep_fraud;
--drop table public.sevl_meta_info;

--drop table public.sevl_stg_transactions;
--drop table public.sevl_stg_terminals;
--drop table public.sevl_stg_passport_blacklist;
--drop table public.sevl_stg_clients;
--drop table public.sevl_stg_accounts;
--drop table public.sevl_stg_cards;


