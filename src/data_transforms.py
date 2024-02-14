# Define data transformations

def wifi_data_transform(df):
    # Address nulls in Station Name column
    df['station_name'].fillna(df['station_complex'], inplace=True)

    # Rename AT&T column
    df['att'] = df['at_t']
    df.drop(columns=['at_t'], inplace=True)

    # NOTE: Postgres automatically coerces Yes and No values (of any case, regardless of whitespace)
    # to booleans if requested, so no need to do so here

    return df
