import codecs
import logging
import pandas as pd
from sqlalchemy import create_engine
import time

# Registrar o alias "utf8mb4" para usar o codec "utf-8"
def search_utf8mb4(encoding_name):
    if encoding_name.lower() == 'utf8mb4':
        return codecs.lookup('utf-8')
    return None

codecs.register(search_utf8mb4)

# Configurar logging para monitorar o progresso do ETL
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configurações do MySQL
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',      
    'port': '3306',           
    'database': 'db_testes',
    'charset': 'utf8mb4'
}

def create_db_engine():
    return create_engine(
        f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}",
        pool_recycle=3600,
        pool_pre_ping=True
    )

engine = create_db_engine()

# Configurações do arquivo CSV
file_path = r'C:\Users\RASS\Documents\Pulse\DADOS\MICRODADOS_ENEM_2020.csv'
delimiter = ';'
encoding = 'utf-8-sig'

# Tamanho do chunk (reduzido para 1000 linhas)
chunksize = 1000


dtype_conversion = {
    'NU_INSCRICAO': str,
    'NU_ANO': int,
    'TP_FAIXA_ETARIA': int,
    'TP_SEXO': str,
    'TP_ESTADO_CIVIL': int,
    'TP_COR_RACA': int,
    'TP_NACIONALIDADE': int,
    'TP_ST_CONCLUSAO': int,
    'TP_ANO_CONCLUIU': int,
    'TP_ESCOLA': int,
    'TP_ENSINO': int,
    'IN_TREINEIRO': int,
    'CO_MUNICIPIO_ESC': float,
    'NO_MUNICIPIO_ESC': str,
    'CO_UF_ESC': float,
    'SG_UF_ESC': str,
    'TP_DEPENDENCIA_ADM_ESC': int,
    'TP_LOCALIZACAO_ESC': int,
    'TP_SIT_FUNC_ESC': int,
    'CO_MUNICIPIO_PROVA': int,
    'NO_MUNICIPIO_PROVA': str,
    'CO_UF_PROVA': int,
    'SG_UF_PROVA': str,
    'TP_PRESENCA_CN': int,
    'TP_PRESENCA_CH': int,
    'TP_PRESENCA_LC': int,
    'TP_PRESENCA_MT': int,
    'CO_PROVA_CN': float,
    'CO_PROVA_CH': float,
    'CO_PROVA_LC': float,
    'CO_PROVA_MT': float,
    'NU_NOTA_CN': float,
    'NU_NOTA_CH': float,
    'NU_NOTA_LC': float,
    'NU_NOTA_MT': float,
    'TX_RESPOSTAS_CN': str,
    'TX_RESPOSTAS_CH': str,
    'TX_RESPOSTAS_LC': str,
    'TX_RESPOSTAS_MT': str,
    'TP_LINGUA': int,
    'TX_GABARITO_CN': str,
    'TX_GABARITO_CH': str,
    'TX_GABARITO_LC': str,
    'TX_GABARITO_MT': str,
    'TP_STATUS_REDACAO': int,
    'NU_NOTA_COMP1': float,
    'NU_NOTA_COMP2': float,
    'NU_NOTA_COMP3': float,
    'NU_NOTA_COMP4': float,
    'NU_NOTA_COMP5': float,
    'NU_NOTA_REDACAO': float,
    'Q001': str,
    'Q002': str,
    'Q003': str,
    'Q004': str,
    'Q005': int,
    'Q006': str,
    'Q007': str,
    'Q008': str,
    'Q009': str,
    'Q010': str,
    'Q011': str,
    'Q012': str,
    'Q013': str,
    'Q014': str,
    'Q015': str,
    'Q016': str,
    'Q017': str,
    'Q018': str,
    'Q019': str,
    'Q020': str,
    'Q021': str,
    'Q022': str,
    'Q023': str,
    'Q024': str,
    'Q025': str
}

db_table = 'nome_da_tabela'

logging.info("Iniciando o processamento do arquivo em chunks...")

with open(file_path, 'r', encoding=encoding, errors='replace') as file_obj:
    reader = pd.read_csv(file_obj, delimiter=delimiter, chunksize=chunksize)
    
    for i, chunk in enumerate(reader, start=1):
        logging.info(f"Processando chunk {i}...")
        
        chunk.columns = [col.replace('ï»¿', '') for col in chunk.columns]
        
        missing_cols = set(dtype_conversion.keys()) - set(chunk.columns)
        if missing_cols:
            logging.error(f"Chunk {i}: Colunas ausentes: {missing_cols}")
            continue

        for col, col_type in dtype_conversion.items():
            if col_type == int:
                chunk[col] = chunk[col].fillna(0)

        try:
            chunk = chunk.astype(dtype_conversion)
        except Exception as e:
            logging.error(f"Chunk {i}: Erro na conversão de tipos - {e}")
            continue

        try:
            mode = 'replace' if i == 1 else 'append'
            chunk.to_sql(db_table, con=engine, if_exists=mode, index=False)
            logging.info(f"Chunk {i} inserido com sucesso.")
        except Exception as e:
            logging.error(f"Chunk {i}: Erro ao inserir no banco de dados - {e}")

            engine.dispose()
            engine = create_db_engine()
            logging.info("Conexão reiniciada após erro.")

            time.sleep(2)

end_time = time.time()  # Finaliza a medição do tempo
elapsed_time = end_time - start_time
logging.info(f"Processamento concluído em {elapsed_time:.2f} segundos.")
