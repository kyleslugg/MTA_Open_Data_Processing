DROP TABLE IF EXISTS open_data.wifi_locations;

CREATE TABLE IF NOT EXISTS open_data.wifi_locations (
  station_complex text,
  station_name text,
  lines text,
  historical boolean,
  borough text,
  county text,
  latitude double precision,
  longitude double precision,
  wifi_available boolean,
  att boolean,
  sprint boolean,
  t_mobile boolean,
  verizon boolean,
  location text,
  georeference geometry(POINT, 4326),
  PRIMARY KEY(station_complex, station_name, lines)
);

DROP TABLE IF EXISTS open_data.subway_ridership;

CREATE TABLE IF NOT EXISTS open_data.subway_ridership (
  transit_timestamp timestamp,
  transit_mode text,
  station_complex_id int,
  station_complex text,
  borough text,
  payment_method text,
  fare_class_category text,
  ridership double precision,
  transfers double precision,
  latitude double precision,
  longitude double precision,
  georeference geometry(POINT, 4326),
  PRIMARY KEY(transit_timestamp, station_complex, payment_method, fare_class_category)
);