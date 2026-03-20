import os
import pyodbc

# --- Configuração de ligação ao SQL Server (adaptada ao TP) ---
MSSQL_HOST   = os.getenv("MSSQL_HOST", "127.0.0.1")
MSSQL_PORT   = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER   = os.getenv("MSSQL_USER", "sa")
MSSQL_PWD    = os.getenv("MSSQL_PWD",  "1234")
MSSQL_DB     = os.getenv("MSSQL_DB",   "TP")  # ou DW_Viagens
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

def get_mssql_conn():
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DB};UID={MSSQL_USER};PWD={MSSQL_PWD};"
        f"TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str)

def estimar_espaco_5_anos():
    conn = get_mssql_conn()
    cur = conn.cursor()

    # 1) nº total de viagens atualmente carregadas
    cur.execute("SELECT COUNT(*) FROM f_viagem;")
    total_atual = cur.fetchone()[0]

    # 2) intervalo de anos coberto (com base na data de partida)
    cur.execute("""
        SELECT MIN(t.ano), MAX(t.ano)
        FROM f_viagem f
        JOIN d_tempo t ON f.d_tempo_partida = t.idtempo;
    """)
    min_ano, max_ano = cur.fetchone()
    anos_historico = (max_ano - min_ano + 1) if min_ano and max_ano else 1

    viagens_por_ano = total_atual / anos_historico
    viagens_5_anos  = viagens_por_ano * 5

    # 3) estimar tamanho médio de uma linha da de factoss (~100 bytes)
    bytes_por_linha = 100
    bytes_5_anos    = viagens_5_anos * bytes_por_linha
    mb_5_anos       = bytes_5_anos / (1024 ** 2)

    print(f"Total atual de viagens: {total_atual}")
    print(f"Anos de histórico: {anos_historico}")
    print(f"Média de viagens/ano: {viagens_por_ano:.2f}")
    print(f"Estimativa de viagens em 5 anos: {viagens_5_anos:.0f}")
    print(f"Espaço estimado (fact, 5 anos): {mb_5_anos:.2f} MB")

    conn.close()

if __name__ == '__main__':
    estimar_espaco_5_anos()
