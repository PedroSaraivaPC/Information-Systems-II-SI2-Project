
import os
import csv
import mysql.connector as mysql
import pyodbc

# Nome do ficheiro CSV
CSV_PATH = os.getenv("CSV_MOCKAROO", "dados_gerados_mockaroo.csv")

# Variáveis de ligação ao SQL Server
MSSQL_HOST   = os.getenv("MSSQL_HOST", "127.0.0.1")        # IP/hostname
MSSQL_PORT   = int(os.getenv("MSSQL_PORT", "1433"))        # Porta do SQL Server
MSSQL_USER   = os.getenv("MSSQL_USER", "sa")               # Utilizador
MSSQL_PWD    = os.getenv("MSSQL_PWD",  "1234")             # Password
MSSQL_DB     = os.getenv("MSSQL_DB",   "TP")           # Nome da BD DW
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")  # Driver instalado

# ligação ao SGBD MSSQL
def get_mssql_conn():
    """
    Liga ao SQL Server via pyodbc (schema padrão: dbo).
    Usa as variáveis MSSQL_* definidas no topo.
    """
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};"
        f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DB};"
        f"UID={MSSQL_USER};PWD={MSSQL_PWD};"
        "TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str)

# Funcoes auxiliares

# para transformar a string do estado do mar para inteiro (para assim adicionar à tabela de factos)
def map_estado_mar_to_int(label):
    
    # Converte o label de estado do mar em código inteiro:
    # 1-Muito Calmo, 2-Calmo, 3-Moderado, 4-Agitado, 5-Tempestuoso
    
    if not label:
        return None

    label = label.strip().lower()

    if "muito calmo" in label:
        return 1
    if "calmo" in label:
        return 2
    if "moderado" in label:
        return 3
    if "agitado" in label:
        return 4
    if "tempest" in label:
        return 5

    return None

# --------------------------------------------

def main():
    print("1 - Ligação ao SQL Server")
    conn = get_mssql_conn()
    cur = conn.cursor()

    try:
        print(f"2 - A abrir CSV Mockaroo: {CSV_PATH}")
        with open(CSV_PATH, mode="r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=",")

            linhas = 0

            for idx, row in enumerate(reader, start=1):

                idv = int(row["id_viagem"])
                estado_int = map_estado_mar_to_int(row["estado_mar"])
                distancia = float(row["distancia_km"].replace(",", "."))

                cur.execute("""
                    UPDATE f_viagem
                    SET estado_mar = ?, distancia_km = ?
                    WHERE idviagem = ?
                """, (estado_int, distancia, idv))

                linhas += 1
                if idx % 500 == 0:
                    conn.commit()
                    print(f"... {idx} linhas processadas")

        conn.commit()
        print(f"ETL CSV Mockaroo concluído! Linhas processadas: {linhas}")
    except FileNotFoundError:
        print(f"ERRO: ficheiro CSV não encontrado: {CSV_PATH}")
        print("Verifica o caminho/nome do ficheiro no script.")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()