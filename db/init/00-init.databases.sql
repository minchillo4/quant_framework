-- Create databases (runs ONLY on fresh volume)
CREATE DATABASE quant_dev;
CREATE DATABASE quant;
CREATE DATABASE airflow;

-- Create role
CREATE ROLE quant_app
  LOGIN
  PASSWORD 'changeme';

-- Create airflow user with appropriate privileges
CREATE ROLE airflow
  LOGIN
  PASSWORD 'airflow123';

-- Grant privileges to airflow user
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Grant schema-level privileges on public schema (required for airflow db upgrade)
\c airflow
GRANT USAGE, CREATE ON SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES FOR USER postgres IN SCHEMA public GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES FOR USER postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;
