CREATE MATERIALIZED VIEW IF NOT EXISTS derived_aggs.daily_ridership_historical_provider
AS
  select
    date_trunc('day',
    sr.transit_timestamp) as transit_date,
    cstats.historical,
    cstats.provider_available,
    sum(sr.ridership) as ridership
  from
    open_data.subway_ridership sr
  join
  (
    select
      station_complex ,
      bool_or(historical) as historical ,
      bool_or(wl.att
      or wl.sprint
      or wl.t_mobile
      or wl.verizon) as provider_available
    from
      open_data.wifi_locations wl
    group by
      station_complex) cstats
  on
    sr.station_complex = cstats.station_complex
  group by
    transit_date,
    cstats.historical,
    cstats.provider_available
WITH NO DATA;

REFRESH MATERIALIZED VIEW derived_aggs.daily_ridership_historical_provider;