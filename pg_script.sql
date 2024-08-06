CREATE SCHEMA report;
CREATE SCHEMA sync;

drop table report.shk_lost_day;
CREATE TABLE IF NOT EXISTS report.shk_lost_day
(
    dt_load             TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dt_date             DATE                        NOT NULL,
    operation_code      VARCHAR(20)                 NOT NULL,
    lostreason_id       INTEGER                     NOT NULL,
    qty_lost            BIGINT                      NOT NULL,
    sum_lost            NUMERIC(15,2)               NOT NULL,
    PRIMARY KEY (dt_date, operation_code, lostreason_id)
);

CREATE OR REPLACE FUNCTION report.shk_lost_day_get(_date_from DATE, _date_to DATE) RETURNS JSONB
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    SET TIME ZONE 'Europe/Moscow';

    RETURN JSONB_BUILD_OBJECT('data', JSONB_AGG(ROW_TO_JSON(res)))
        FROM (SELECT dt_date,
                     operation_code,
                     lostreason_id,
                     qty_lost,
                     sum_lost
              FROM report.shk_lost_day ass
              WHERE ass.dt_date >= _date_from::TIMESTAMP
                AND ass.dt_date < _date_to::TIMESTAMP + interval '1 day'
              ) res;
END;
$$;

CREATE OR REPLACE PROCEDURE sync.shk_lost_day_importfromclick(_src JSONB)
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    WITH cte AS (SELECT DISTINCT ON (src.dt_date, src.operation_code, src.lostreason_id) src.dt_load,
                                                             src.dt_date,
                                                             src.operation_code,
                                                             src.lostreason_id,
                                                             src.qty_lost,
                                                             src.sum_lost
                 FROM JSONB_TO_RECORDSET(_src) AS src(dt_load TIMESTAMP WITHOUT TIME ZONE,
                                                      dt_date        DATE,
                                                      operation_code VARCHAR(20),
                                                      lostreason_id  INTEGER,
                                                      qty_lost       BIGINT,
                                                      sum_lost       NUMERIC(15,2))
                 ORDER BY src.dt_date, src.operation_code, src.lostreason_id, src.dt_load DESC)
    INSERT
    INTO report.shk_lost_day AS ass(dt_load,
                                     dt_date,
                                     operation_code,
                                     lostreason_id,
                                     qty_lost,
                                     sum_lost)
    SELECT c.dt_load, c.dt_date, c.operation_code, c.lostreason_id, c.qty_lost, c.sum_lost
    FROM cte c
    ON CONFLICT (dt_date, operation_code, lostreason_id) DO UPDATE
        SET qty_lost = excluded.qty_lost,
            sum_lost = excluded.sum_lost,
            dt_load  = excluded.dt_load
    WHERE ass.dt_load < excluded.dt_load;
END;
$$;

select * from report.shk_lost_day;
