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
import math
from typing import Dict, List


@dag(
    dag_id="pipeline_produtos_v3",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "labdados", "produtos", "geografico"],
    description="Pipeline ETL para dados de produtos - Arquitetura Medallion com Análise Geográfica",
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

    def validar_e_limpar_coordenadas(df):
        """Valida e limpa coordenadas geográficas"""
        
        # Dicionário com coordenadas aproximadas dos centros dos estados brasileiros
        coordenadas_estados = {
            'AC': (-8.77, -70.55), 'AL': (-9.71, -35.73), 'AP': (1.41, -51.77),
            'AM': (-3.07, -61.66), 'BA': (-12.96, -38.51), 'CE': (-3.71, -38.54),
            'DF': (-15.83, -47.86), 'ES': (-19.19, -40.34), 'GO': (-16.64, -49.31),
            'MA': (-2.55, -44.30), 'MT': (-12.64, -55.42), 'MS': (-20.51, -54.54),
            'MG': (-18.10, -44.38), 'PA': (-5.53, -52.29), 'PB': (-7.06, -35.55),
            'PR': (-24.89, -51.55), 'PE': (-8.28, -35.07), 'PI': (-8.28, -43.68),
            'RJ': (-22.84, -43.15), 'RN': (-5.22, -36.52), 'RS': (-30.01, -51.22),
            'RO': (-11.22, -62.80), 'RR': (1.99, -61.33), 'SC': (-27.33, -49.44),
            'SP': (-23.55, -46.64), 'SE': (-10.90, -37.07), 'TO': (-10.25, -48.25)
        }
        
        # Função para validar se coordenadas estão no Brasil
        def coordenadas_no_brasil(lat, lon):
            return (-33 <= lat <= 5) and (-74 <= lon <= -32)
        
        # Limpar coordenadas inválidas
        df_limpo = df.copy()
        df_limpo['coordenada_aproximada'] = False
        
        # Substituir coordenadas inválidas pela coordenada do centro do estado
        for idx, row in df_limpo.iterrows():
            lat, lon = row.get('lat'), row.get('lon')
            estado = row.get('Local da compra', '').upper()
            
            # Se coordenadas são inválidas ou estão fora do Brasil
            if (pd.isna(lat) or pd.isna(lon) or 
                not coordenadas_no_brasil(lat, lon)):
                
                # Usar coordenada do centro do estado se disponível
                if estado in coordenadas_estados:
                    df_limpo.at[idx, 'lat'] = coordenadas_estados[estado][0]
                    df_limpo.at[idx, 'lon'] = coordenadas_estados[estado][1]
                    df_limpo.at[idx, 'coordenada_aproximada'] = True
        
        return df_limpo

    def enriquecer_dados_geograficos(df):
        """Adiciona informações geográficas extras"""
        
        # Mapeamento de regiões
        regioes = {
            'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
            'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
            'Centro-Oeste': ['GO', 'MT', 'MS', 'DF'],
            'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
            'Sul': ['PR', 'RS', 'SC']
        }
        
        # Adicionar região
        def obter_regiao(estado):
            for regiao, estados in regioes.items():
                if estado.upper() in estados:
                    return regiao
            return 'Outros'
        
        df['regiao'] = df['Local da compra'].apply(obter_regiao)
        
        # Adicionar informações de distância (exemplo: distância de São Paulo)
        def calcular_distancia_sp(lat, lon):
            """Calcula distância aproximada de São Paulo"""
            lat_sp, lon_sp = -23.55, -46.64
            
            # Fórmula haversine simplificada
            dlat = math.radians(lat - lat_sp)
            dlon = math.radians(lon - lon_sp)
            a = (math.sin(dlat/2)**2 + 
                 math.cos(math.radians(lat_sp)) * math.cos(math.radians(lat)) * 
                 math.sin(dlon/2)**2)
            c = 2 * math.asin(math.sqrt(a))
            r = 6371  # Raio da Terra em km
            return c * r
        
        df['distancia_sp_km'] = df.apply(
            lambda row: calcular_distancia_sp(row['lat'], row['lon']), 
            axis=1
        )
        
        return df

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
            
            # Limpar e validar coordenadas geográficas
            df_limpo = validar_e_limpar_coordenadas(df_limpo)
            
            # Enriquecer com dados geográficos
            df_limpo = enriquecer_dados_geograficos(df_limpo)
            
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
    def criar_mart_geografico(resultado_silver: Dict[str, int]) -> Dict[str, int]:
        """Cria mart específico para visualizações geográficas"""
        logging.info("Iniciando criação do mart geográfico")
        
        try:
            hook = PostgresHook(postgres_conn_id="postgres_analytics")
            engine = hook.get_sqlalchemy_engine()
            
            # Carregar dados limpos
            df = pd.read_sql("SELECT * FROM silver.produtos_clean", engine)
            
            if df.empty:
                logging.warning("Nenhum dado disponível no Silver")
                return {"mart_geografico_criado": 0}
            
            # Filtrar apenas registros com coordenadas válidas
            df_geo = df.dropna(subset=['lat', 'lon']).copy()
            
            # Validar coordenadas (Brasil: lat -33 a 5, lon -74 a -32)
            df_geo = df_geo[
                (df_geo['lat'].between(-33, 5)) & 
                (df_geo['lon'].between(-74, -32))
            ]
            
            # Criar agregações por estado com coordenadas médias
            mart_geo = df_geo.groupby(['Local da compra', 'regiao']).agg({
                'Preço': ['sum', 'count', 'mean'],
                'Avaliação da compra': 'mean',
                'lat': 'mean',  # Coordenada média do estado
                'lon': 'mean',
                'Frete': 'mean',
                'distancia_sp_km': 'mean'
            }).round(4)
            
            # Achatar colunas
            mart_geo.columns = [
                'total_vendas', 'qtd_vendas', 'ticket_medio', 
                'avaliacao_media', 'latitude', 'longitude', 'frete_medio',
                'distancia_media_sp'
            ]
            
            mart_geo = mart_geo.reset_index()
            mart_geo['atualizado_em'] = pd.Timestamp.now()
            
            # Adicionar ranking de vendas
            mart_geo['ranking_vendas'] = mart_geo['total_vendas'].rank(ascending=False)
            
            # Categorizar performance
            mart_geo['categoria_performance'] = pd.cut(
                mart_geo['total_vendas'], 
                bins=3, 
                labels=['Baixa', 'Média', 'Alta']
            )
            
            # Salvar mart geográfico
            mart_geo.to_sql(
                "mart_vendas_geografico", 
                engine, 
                schema="gold",
                if_exists="replace", 
                index=False
            )
            
            logging.info("Mart geográfico criado com sucesso")
            return {"mart_geografico_criado": 1}
            
        except Exception as e:
            logging.error(f"Erro na criação do mart geográfico: {str(e)}")
            raise

    @task()
    def criar_kpis_dashboard(resultado_silver: Dict[str, int]) -> Dict[str, int]:
        """Cria KPIs consolidados para dashboard"""
        logging.info("Iniciando criação de KPIs")
        
        try:
            hook = PostgresHook(postgres_conn_id="postgres_analytics")
            engine = hook.get_sqlalchemy_engine()
            
            # Carregar dados limpos
            df = pd.read_sql("SELECT * FROM silver.produtos_clean", engine)
            
            if df.empty:
                logging.warning("Nenhum dado disponível no Silver")
                return {"kpis_criados": 0}
            
            # Calcular KPIs principais
            kpis = {
                'total_vendas': df['Preço'].sum(),
                'total_pedidos': len(df),
                'ticket_medio': df['Preço'].mean(),
                'avaliacao_media': df['Avaliação da compra'].mean(),
                'frete_medio': df['Frete'].mean(),
                'total_estados': df['Local da compra'].nunique(),
                'total_produtos': df['Produto'].nunique(),
                'total_categorias': df['Categoria do Produto'].nunique(),
                'total_vendedores': df['Vendedor'].nunique(),
                'periodo_inicio': df['Data da Compra'].min(),
                'periodo_fim': df['Data da Compra'].max(),
                'atualizado_em': pd.Timestamp.now()
            }
            
            # Converter para DataFrame
            kpis_df = pd.DataFrame([kpis])
            
            # KPIs por período (últimos 30 dias vs anterior)
            data_corte = df['Data da Compra'].max() - pd.Timedelta(days=30)
            df_recente = df[df['Data da Compra'] >= data_corte]
            df_anterior = df[df['Data da Compra'] < data_corte]
            
            if not df_recente.empty and not df_anterior.empty:
                crescimento_vendas = ((df_recente['Preço'].sum() / df_anterior['Preço'].sum()) - 1) * 100
                crescimento_pedidos = ((len(df_recente) / len(df_anterior)) - 1) * 100
                
                kpis_df['crescimento_vendas_30d'] = crescimento_vendas
                kpis_df['crescimento_pedidos_30d'] = crescimento_pedidos
            
            # Salvar KPIs
            kpis_df.to_sql(
                "kpis_dashboard", 
                engine, 
                schema="gold",
                if_exists="replace", 
                index=False
            )
            
            # KPIs por Estado (Top 10)
            kpis_estado = df.groupby('Local da compra').agg({
                'Preço': ['sum', 'count', 'mean'],
                'Avaliação da compra': 'mean',
                'Frete': 'mean'
            }).round(2)
            
            kpis_estado.columns = ['total_vendas', 'qtd_pedidos', 'ticket_medio', 'avaliacao_media', 'frete_medio']
            kpis_estado = kpis_estado.reset_index()
            kpis_estado['participacao_vendas'] = (kpis_estado['total_vendas'] / kpis_estado['total_vendas'].sum() * 100).round(2)
            kpis_estado = kpis_estado.sort_values('total_vendas', ascending=False).head(10)
            kpis_estado['atualizado_em'] = pd.Timestamp.now()
            
            kpis_estado.to_sql(
                "kpis_top_estados", 
                engine, 
                schema="gold",
                if_exists="replace", 
                index=False
            )
            
            # KPIs por Categoria
            kpis_categoria = df.groupby('Categoria do Produto').agg({
                'Preço': ['sum', 'count', 'mean'],
                'Avaliação da compra': 'mean'
            }).round(2)
            
            kpis_categoria.columns = ['total_vendas', 'qtd_pedidos', 'ticket_medio', 'avaliacao_media']
            kpis_categoria = kpis_categoria.reset_index()
            kpis_categoria['participacao_vendas'] = (kpis_categoria['total_vendas'] / kpis_categoria['total_vendas'].sum() * 100).round(2)
            kpis_categoria = kpis_categoria.sort_values('total_vendas', ascending=False)
            kpis_categoria['atualizado_em'] = pd.Timestamp.now()
            
            kpis_categoria.to_sql(
                "kpis_categorias", 
                engine, 
                schema="gold",
                if_exists="replace", 
                index=False
            )
            
            logging.info("KPIs criados com sucesso")
            return {"kpis_criados": 3}
            
        except Exception as e:
            logging.error(f"Erro na criação de KPIs: {str(e)}")
            raise

    @task()
    def log_pipeline_stats(stats_extracao, stats_silver, stats_gold, stats_geo, stats_kpis):
        """Log das estatísticas do pipeline"""
        logging.info("=== ESTATÍSTICAS DO PIPELINE ===")
        logging.info(f"Registros extraídos: {stats_extracao.get('registros_extraidos', 0)}")
        logging.info(f"Registros processados (Silver): {stats_silver.get('registros_processados', 0)}")
        logging.info(f"Marts criados (Gold): {stats_gold.get('marts_criados', 0)}")
        logging.info(f"Mart geográfico criado: {stats_geo.get('mart_geografico_criado', 0)}")
        logging.info(f"KPIs criados: {stats_kpis.get('kpis_criados', 0)}")
        logging.info("Pipeline executado com sucesso!")

    # Definir dependências
    start = EmptyOperator(task_id="inicio_pipeline")
    end = EmptyOperator(task_id="fim_pipeline")
    
    # Execução das tasks
    stats_extracao = extrair_dados_api()
    stats_silver = transformar_para_silver(stats_extracao)
    stats_gold = criar_marts_gold(stats_silver)
    stats_geo = criar_mart_geografico(stats_silver)
    stats_kpis = criar_kpis_dashboard(stats_silver)
    pipeline_stats = log_pipeline_stats(stats_extracao, stats_silver, stats_gold, stats_geo, stats_kpis)
    
    # Dependências
    start >> criar_schemas >> criar_tabelas >> stats_extracao
    stats_extracao >> stats_silver
    stats_silver >> [stats_gold, stats_geo, stats_kpis]
    [stats_gold, stats_geo, stats_kpis] >> pipeline_stats >> end

# Instanciar a DAG
etl_produtos = etl_produtos_pipeline()
