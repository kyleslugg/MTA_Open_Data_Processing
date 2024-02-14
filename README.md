# Processing MTA Open Data: Exploration and Pipeline Construction

This repository contains the necessary logic (and, through provision of docker containers, baseline infrastructure) to obtain, process, and store two open datasets provided by the Metropolitan Transportation Authority: [hourly subway ridership data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s) across several dimensions, and the data on [the presence of wifi](https://data.ny.gov/Transportation/MTA-Wi-Fi-Locations/pwa9-tmie) within the NYC Subway system.

As explained below, this repository contains two methods for obtaining and parsing these data (in chunks, with paging). The first, a simple, sequential Python script, was developed as a draft of sorts, to test the underlying logic. The second, an Apache Airflow DAG, was created to parallelize and decouple the data loading tasks, in particular. (It was also not lost on me that the MTA [uses Airflow](https://new.mta.info/article/how-we-build-analytics-scale-mta) in its own pipelines, and this seemed as good a chance as any to experiment.)

In both cases, the data are stored in a Postgres database, which is equipped with PostGIS to handle georeferences and enable future spatial querying. Each dataset is stored, after minor transformations, in a table of its own under the `open_data` schema: because these data will be used for analytics, I deemed any extra disk space consumed by the duplication of certain fields to be worth potential performance gains associated with avoiding unnecessary cross-table joins, and so did not pursue further normalization. **The schemata for these tables may be found in `./sql/db_init/table_init.sql`.**

Aggregated daily ridership data (split by a station complex's historical status and the availability of cellular networks) is held in the `derived_aggs` schema as a materialized view on the above tables, which is refreshed with each run of the workflow here. (Note that, during this aggregation, a station complex was deemed to be "historical" and to have a "provider available" if these conditions applied to any of its constituent parts, as found in the wifi locations dataset.) **This view is defined in `./sql/agg_view.sql`.**

## Setup and Execution

This repository provides two ways to execute this pipeline: a sequential script created as a draft (the "Draft Workflow"), and an Apache Airflow DAG. Both require the presence of:

1. Socrata API credentials, stored in an environment file (see `.env.example` for structure, and rename to `.env`)
2. A PostgreSQL database equipped with the PostGIS extension (buildable from `postgis.Dockerfile`), initialized using the scripts contained in `./sql/db_init/`.

Specific instructions are as follows:

### Draft Workflow

1. Ensure that a PostGIS-equipped Postgres database is accessible via port 5432 on the local host. See `./src/constants.py` for the default credentials and databse name expected. A database meeting these specifications can be built from `postgis.Dockerfile`; the image port 5432 should be mapped to 5432 on the local host.
2. Ensure all required Python packages (contained in `requirements.txt`) are installed. Note that SQLAlchemy 2.0+ is required; this may create conflicts if this routine is run in the same Python environment in which Airflow is installed
3. From the project root, execute `python ./src/run_draft_pipeline.py`

### Airflow Orchestration

The following steps can be completed in one fell swoop by executing `docker compose build | docker compose up` in the project root, provided that Docker Engine is available to you, and the resulting database viewed on port 5432.

However, for the sake of clarity:

1. Establish Airflow Connections to Postgres/PostGIS Database. See `docker-compose.yml` for an example.
   1. Ensure that the PostGIS database described above is accessible as an Airflow Connection named `postgis`. Note that this connection should be specified in a form that is compatible with SQLAlchemy 2.0+. That is, the connection should be of type `postgresql+psycopg2://...`, as opposed to `postgres`.
   2. A second Airflow Connection should be defined to this same database under the name `postgis_af`, this time with a type of `postgres://...`.
2. Place `./dags/mta_pipeline.py` in your local `dags` directory, and place the `sql` and `src` directories as siblings to that directory.
3. Create a Python virtual environment that contains all packages in `requirements.txt`, and place in a directory `pg_venv` that is a sibling to your `dags` directory.
4. Refresh your DAG list, and watch it run!
