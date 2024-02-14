FROM apache/airflow:2.8.1-python3.11
RUN pip install apache-airflow[postgres] connexion[swagger-ui]
COPY .env /opt/airflow/

# Set up venv
ENV PIP_USER=false
RUN python3 -m venv /opt/airflow/pg_venv

COPY requirements.txt .
RUN /opt/airflow/pg_venv/bin/pip install -r requirements.txt

ENV PIP_USER=true