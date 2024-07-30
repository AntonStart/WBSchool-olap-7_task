-- СОЗДАНИЕ схем
create schema report;
create schema sync;

-- СОЗДАНИЕ витрины в pg
drop table if exists report.tareTransfer_agg;
create table if not exists report.tareTransfer_agg
(
    place_cod               integer not null,
    wh_tare_status_type     varchar(10),
    wh_tare_entry     varchar(10),
    dt_date   date    not null,
    qty_tares integer not null,
primary key (place_cod)
);

-- Индексы, для replacing замены данных, при использовании процедур
CREATE UNIQUE INDEX idx_taretransfer_agg_unique
ON report.taretransfer_agg (place_cod, dt_date, wh_tare_entry, wh_tare_status_type);

-- Процедура импорта
create or replace procedure sync.taretransfer_agg_insert(_src json)
    security definer
    language plpgsql
as
    $$
        begin
            insert into report.taretransfer_agg as main(place_cod
                                                      , dt_date
                                                      , wh_tare_entry
                                                      , wh_tare_status_type
                                                      , qty_tares)
            select distinct on (s.place_cod) s.place_cod,
                                             s.dt_date,
                                             s.wh_tare_entry,
                                             s.wh_tare_status_type,
                                             s.qty_tares
            from json_to_recordset(_src) as s(
                                                place_cod integer
                                              , dt_date date
                                              , wh_tare_entry varchar(10)
                                              , wh_tare_status_type varchar(10)
                                              , qty_tares integer
                                            )
            on conflict (place_cod, dt_date, wh_tare_entry, wh_tare_status_type) do update
            set qty_tares = excluded.qty_tares
            where main.qty_tares < excluded.qty_tares;
        end;
    $$;
