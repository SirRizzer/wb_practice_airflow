CREATE TABLE default.shk_lost
(
    dt_operation             DateTime64(3),
    operation_code           LowCardinality(String),
    lostreason_id            UInt32,
    shk_id                   Int64,
    amount                   Decimal(15, 2)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt_operation)
ORDER BY shk_id
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 72000, ttl_only_drop_parts = 1;

create database report;

create table if not exists report.shk_lost_day (
    dt_date Date,
    operation_code           LowCardinality(String),
    lostreason_id            UInt32,
    qty_lost UInt32,
    sum_lost Decimal(15, 2),
    dt_load DateTime materialized now()
)
engine = ReplacingMergeTree()
partition by toYYYYMM(dt_date)
order by (dt_date, operation_code, lostreason_id)
ttl dt_date + interval 30 day;

select *
from report.shk_lost_day
