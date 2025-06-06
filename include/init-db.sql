-- Criar banco para o Metabase
CREATE DATABASE metabase_app;

-- Configurar o banco analytics para o pipeline
\c analytics;

-- Criar schemas para arquitetura Medallion
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver; 
CREATE SCHEMA IF NOT EXISTS gold;

-- Criar usuário específico para o Airflow 
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';

-- Conceder permissões
GRANT CONNECT ON DATABASE analytics TO airflow_user;
GRANT USAGE ON SCHEMA bronze, silver, gold TO airflow_user;
GRANT CREATE ON SCHEMA bronze, silver, gold TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze, silver, gold TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze, silver, gold TO airflow_user;

-- Permissões para tabelas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON SEQUENCES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON SEQUENCES TO airflow_user;
