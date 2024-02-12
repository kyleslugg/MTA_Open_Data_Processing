
import pandas as pd
import geopandas as gpd
import shapely.geometry
import geoalchemy2
from sqlalchemy import create_engine, text, Engine, Connection
from sqlalchemy.engine.url import URL

DEFAULT_ENGINE_PARAMS = {'pool_size': 5, 'pool_recycle': 3600}


def get_pg_db_engine(db_params, engine_params=DEFAULT_ENGINE_PARAMS) -> Engine:
    url_params = dict(
        {"drivername": "postgresql+psycopg2"}, **db_params)

    db_url = URL.create(**url_params)
    return create_engine(db_url, **engine_params)


def execute_pg_file(path, conn: Connection):
    with open(path, 'r') as reader:
        sql_text = reader.read()
        conn.execute(text(sql_text))
        conn.commit()


def to_geom(x):
    try:
        return shapely.geometry.shape(x)
    except:
        print(x)


def save_socrata_dataset(data, db: Engine, table_name, schema_name, data_transform_function=None, geometry_col=None, **kwargs):

    # Read dataset chunk by chunk, and append each new chunk to the same table if it already exists
    with db.connect() as conn:
        for chunk in data:
            # Declare df
            if geometry_col:
                df = gpd.GeoDataFrame.from_dict(chunk)
                df[geometry_col] = df[geometry_col].apply(
                    lambda x: to_geom(x))
                df.set_geometry(geometry_col, inplace=True)
                df.set_crs(epsg=4326, inplace=True)
                print(df)

            else:

                df = pd.DataFrame.from_dict(chunk)

            # Apply data transformations if specified
            if data_transform_function:
                df = data_transform_function(df)

            if geometry_col:
                df.to_postgis(table_name, conn, if_exists='append',
                              schema=schema_name, **kwargs)
            else:
                df.to_sql(table_name, conn, if_exists='append',
                          schema=schema_name, **kwargs)
