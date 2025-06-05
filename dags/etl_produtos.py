from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from datetime import timedelta
import requests
import json
import pandas as pd
import logging
from typing import Dict, List


@dag(
    dag_id="pipeline_produtos_v2",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Mudança aqui: schedule em vez de schedule_interval
    catchup=False,
    tags=["etl", "labdados", "produtos"],
    description="Pipeline ETL para dados de produtos - Arquitetura Medallion",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def etl_produtos_pipeline():
    
    # 🏗️ Criar infraestrutura
    criar_schemas = PostgresOperator(
        task_id="criar_schemas",
        postgres_conn_id="postgres_analytics",
        sql="""
            -- Bronze Layer (Raw Data)
            CREATE SCHEMA IF NOT EXISTS bronze;
            
            -- Silver Layer (Clean Data)  
            CREATE SCHEMA IF NOT EXISTS silver;
            
            -- Gold Layer (Business Ready)
            CREATE SCHEMA IF NOT EXISTS gold;
        """,
    )
    
    criar_tabelas = PostgresOperator(
        task_id="criar_tabelas",
        postgres_conn_id="postgres_analytics",
        sql="""
            -- Tabela bronze para dados raw
            CREATE TABLE IF NOT EXISTS bronze.raw_produtos (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL,
                ingestion_time TIMESTAMP DEFAULT NOW(),
                data_particao DATE DEFAULT CURRENT_DATE
            );
            
            -- Índices para performance
            CREATE INDEX IF NOT EXISTS idx_raw_produtos_data_particao 
            ON bronze.raw_produtos(data_particao);
            
            CREATE INDEX IF NOT EXISTS idx_raw_produtos_ingestion 
            ON bronze.raw_produtos(ingestion_time);
        """,
    )

    @task()
    def extrair_dados_api() -> Dict[str, int]:
        """Extrai dados da API e salva na camada Bronze"""
        logging.info("Iniciando extração de dados da API")
        
        try:
            url = "https://labdados.com/produtos"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Validação básica
            if not isinstance(data, list) or len(data) == 0:
                raise ValueError("API retornou dados inválidos ou vazios")
            
            logging.info(f"Extraídos {len(data)} registros da API")
            
            # Inserir no PostgreSQL
            hook = PostgresHook(postgres_conn_id="postgres_analytics")
            
            # Limpar dados do dia atual antes de inserir novos
            delete_sql = """
                DELETE FROM bronze.raw_produtos 
                WHERE data_particao = CURRENT_DATE
            """
            hook.run(delete_sql)
            
            # Inserir novos dados
            insert_sql = """
                INSERT INTO bronze.raw_produtos (data, ingestion_time, data_particao)
                VALUES (%s, NOW(), CURRENT_DATE)
            """
            
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            for item in data:
                cursor.execute(insert_sql, (json.dumps(item),))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"Inseridos {len(data)} registros na camada Bronze")
            return {"registros_extraidos": len(data)}
            
        except Exception as e:
            logging.error(f"Erro na extração: {str(e)}")
            raise

    @task()
    def transformar_para_silver(resultado_extracao: Dict[str, int]) -> Dict[str, int]:
        """Processa dados raw para camada Silver com limpeza e padronização"""
        logging.info("Iniciando transformação para camada Silver")
        
        try:
            hook = PostgresHook(postgres_conn_id="postgres_analytics")
            engine = hook.get_sqlalchemy_engine()
            
            # Buscar dados do bronze do dia atual
            query = """
                SELECT id, data::jsonb, ingestion_time
                FROM bronze.raw_produtos
                WHERE data_particao = CURRENT_DATE
                ORDER BY id
            """
            
            df = pd.read_sql(query, engine)
            logging.info(f"Carregados {len(df)} registros do Bronze")
            
            if df.empty:
                logging.warning("Nenhum dado encontrado no Bronze para processar")
                return {"registros_processados": 0}
            
            # Normalizar JSON em colunas
            df_normalizado = pd.json_normalize(df['data'])
            
            # Adicionar metadados
            df_normalizado['bronze_id'] = df['id']
            df_normalizado['processed_at'] = pd.Timestamp.now()
            
            # Limpeza e padronização
            df_limpo = df_normalizado.copy()
            
            # Converter data para formato padrão
            df_limpo['Data da Compra'] = pd.to_datetime(
                df_limpo['Data da Compra'], 
                format='%d/%m/%Y'
            )
            
            # Padronizar strings
            cols_string = ['Produto', 'Categoria do Produto', 'Vendedor', 
                          'Local da compra', 'Tipo de pagamento']
            for col in cols_string:
                if col in df_limpo.columns:
                    df_limpo[col] = df_limpo[col].str.strip().str.title()
            
            # Validar dados numéricos
            numeric_cols = ['Preço', 'Frete', 'Avaliação da compra', 
                           'Quantidade de parcelas', 'lat', 'lon']
            for col in numeric_cols:
                if col in df_limpo.columns:
                    df_limpo[col] = pd.to_numeric(df_limpo[col], errors='coerce')
            
            # Remover duplicatas
            df_limpo = df_limpo.drop_duplicates(
                subset=['Produto', 'Data da Compra', 'Vendedor']
            )
            
            # Salvar na camada Silver
            df_limpo.to_sql(
                "produtos_clean",
                engine,
                schema="silver",
                if_exists="replace",
                index=False,
                chunksize=1000
            )
            
            logging.info(f"Processados {len(df_limpo)} registros para Silver")
            return {"registros_processados": len(df_limpo)}
            
        except Exception as e:
            logging.error(f"Erro na transformação Silver: {str(e)}")
            raise

    @task()
    def criar_marts_gold(resultado_silver: Dict[str, int]) -> Dict[str, int]:
        """Cria tabelas analíticas na camada Gold"""
        logging.info("Iniciando criação de marts na camada Gold")
        
        try:
            hook = PostgresHook(postgres_conn_id="postgres_analytics")
            engine = hook.get_sqlalchemy_engine()
            
            # Carregar dados limpos
            df = pd.read_sql("SELECT * FROM silver.produtos_clean", engine)
            
            if df.empty:
                logging.warning("Nenhum dado disponível no Silver")
                return {"marts_criados": 0}
            
            marts_criados = 0
            
            # 1. Vendas por Categoria
            vendas_categoria = (
                df.groupby("Categoria do Produto")
                .agg({
                    "Preço": ["sum", "mean", "count"],
                    "Avaliação da compra": "mean"
                })
                .round(2)
            )
            vendas_categoria.columns = [
                "total_vendas", "preco_medio", "qtd_vendas", "avaliacao_media"
            ]
            vendas_categoria = vendas_categoria.reset_index()
            vendas_categoria["atualizado_em"] = pd.Timestamp.now()
            
            vendas_categoria.to_sql(
                "mart_vendas_categoria", engine, schema="gold", 
                if_exists="replace", index=False
            )
            marts_criados += 1
            
            # 2. Performance por Estado
            performance_estado = (
                df.groupby("Local da compra")
                .agg({
                    "Preço": ["sum", "count"],
                    "Avaliação da compra": "mean",
                    "Frete": "mean"
                })
                .round(2)
            )
            performance_estado.columns = [
                "total_vendas", "qtd_vendas", "avaliacao_media", "frete_medio"
            ]
            performance_estado = performance_estado.reset_index()
            performance_estado["atualizado_em"] = pd.Timestamp.now()
            
            performance_estado.to_sql(
                "mart_performance_estado", engine, schema="gold",
                if_exists="replace", index=False
            )
            marts_criados += 1
            
            # 3. Análise Temporal
            df['ano_mes'] = df['Data da Compra'].dt.to_period('M').astype(str)
            vendas_tempo = (
                df.groupby('ano_mes')
                .agg({
                    "Preço": ["sum", "count"],
                    "Avaliação da compra": "mean"
                })
                .round(2)
            )
            vendas_tempo.columns = ["total_vendas", "qtd_vendas", "avaliacao_media"]
            vendas_tempo = vendas_tempo.reset_index()
            vendas_tempo["atualizado_em"] = pd.Timestamp.now()
            
            vendas_tempo.to_sql(
                "mart_vendas_temporal", engine, schema="gold",
                if_exists="replace", index=False
            )
            marts_criados += 1
            
            # 4. Top Produtos
            top_produtos = (
                df.groupby("Produto")
                .agg({
                    "Preço": ["sum", "count"],
                    "Avaliação da compra": "mean"
                })
                .round(2)
            )
            top_produtos.columns = ["total_vendas", "qtd_vendas", "avaliacao_media"]
            top_produtos = top_produtos.reset_index()
            top_produtos = top_produtos.sort_values("total_vendas", ascending=False).head(20)
            top_produtos["atualizado_em"] = pd.Timestamp.now()
            
            top_produtos.to_sql(
                "mart_top_produtos", engine, schema="gold",
                if_exists="replace", index=False
            )
            marts_criados += 1
            
            logging.info(f"Criados {marts_criados} marts na camada Gold")
            return {"marts_criados": marts_criados}
            
        except Exception as e:
            logging.error(f"Erro na criação de marts Gold: {str(e)}")
            raise

    @task()
    def log_pipeline_stats(stats_extracao, stats_silver, stats_gold):
        """Log das estatísticas do pipeline"""
        logging.info("=== ESTATÍSTICAS DO PIPELINE ===")
        logging.info(f"Registros extraídos: {stats_extracao.get('registros_extraidos', 0)}")
        logging.info(f"Registros processados (Silver): {stats_silver.get('registros_processados', 0)}")
        logging.info(f"Marts criados (Gold): {stats_gold.get('marts_criados', 0)}")
        logging.info("Pipeline executado com sucesso!")

    # Definir dependências
    start = EmptyOperator(task_id="inicio_pipeline")
    end = EmptyOperator(task_id="fim_pipeline")
    
    # Execução das tasks
    stats_extracao = extrair_dados_api()
    stats_silver = transformar_para_silver(stats_extracao)
    stats_gold = criar_marts_gold(stats_silver)
    pipeline_stats = log_pipeline_stats(stats_extracao, stats_silver, stats_gold)
    
    # Dependências
    start >> criar_schemas >> criar_tabelas >> stats_extracao
    stats_extracao >> stats_silver >> stats_gold >> pipeline_stats >> end

# Instanciar a DAG
etl_produtos = etl_produtos_pipeline()
