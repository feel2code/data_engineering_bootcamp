--cdm project
drop table if exists cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
    id                      SERIAL,
    courier_id              INTEGER         NOT NULL,
    courier_name            VARCHAR         NOT NULL,
    settlement_year         INTEGER         NOT NULL,
    settlement_month        INTEGER         NOT NULL,
    orders_count            INTEGER         NOT NULL DEFAULT 0,
    orders_total_sum        NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    rate_avg                NUMERIC(3, 2)   NOT NULL DEFAULT 0,
    order_processing_fee    NUMERIC(14,2)   NOT NULL DEFAULT 0,
    courier_order_sum       NUMERIC(14,2)   NOT NULL DEFAULT 0,
    courier_tips_sum        NUMERIC(14,2)   NOT NULL DEFAULT 0,
    courier_reward_sum      NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
    CONSTRAINT unique_dm_courier_ledger UNIQUE (courier_id, settlement_year, settlement_month),
    CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (settlement_year BETWEEN 2022 AND 29999),
    CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (settlement_month BETWEEN 1 AND 12),
    CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0),
    CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (rate_avg BETWEEN 0 AND 5.00),
    CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK (courier_order_sum BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK (courier_tips_sum BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK (courier_reward_sum BETWEEN 0 AND 999999999999.99)
);