-- Local demo credentials; the business role cannot access Airflow metadata.
CREATE USER orders WITH PASSWORD 'orders';
CREATE DATABASE orders OWNER orders;
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
REVOKE CONNECT ON DATABASE orders FROM PUBLIC;
GRANT CONNECT ON DATABASE orders TO orders;
