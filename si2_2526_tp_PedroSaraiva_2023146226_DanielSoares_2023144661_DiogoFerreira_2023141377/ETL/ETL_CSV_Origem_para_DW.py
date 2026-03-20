
import os
import csv
import pyodbc
from datetime import datetime
import random

# Nome do ficheiro CSV
CSV_PATH = os.getenv("CSV_ORIGEM", "dados_atualizado.csv")

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

# para adicionar o tamanho do barco na dimensao do barco aleatoriamente
def gerar_tamanho_barco():
    """
    Devolve um tamanho de barco entre 70 e 400 (float). Tal como varia no mysql.
    """
    return round(random.uniform(70, 400), 1)

"""
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
"""

# --------------- FUNÇÕES AUXILIARES DIMENSÕES ---------------

def get_or_create_dim_barco(cur, nomebarco, tipobarco, capacidadeteu, paisbarco):
    # tentar encontrar combinação existente
    cur.execute("""
        SELECT idbarco FROM d_barco
        WHERE nome = ? AND tipo = ? AND capacidadeteu = ?
    """, (nomebarco, tipobarco, capacidadeteu))
    row = cur.fetchone()
    if row:
        return row[0]

    # criar novo id
    cur.execute("SELECT ISNULL(MAX(idbarco),0) + 1 FROM d_barco")
    new_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO d_barco (idbarco, nome, tipo, tamanho, pais, capacidadeteu, nomeempresa)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        new_id,
        nomebarco,
        tipobarco,
        gerar_tamanho_barco(),
        paisbarco,
        capacidadeteu,
        "Empresa Desconhecida (csv)"
    ))

    # validação
    cur.execute("SELECT idbarco FROM d_barco WHERE idbarco = ?", (new_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao obter idbarco após INSERT em d_barco.")
    return row[0]

def get_or_create_dim_condutor(cur, nomecond, idade, certif, sexo_raw):
    sexo = sexo_raw.strip().upper()

    cur.execute("""
        SELECT idcondutor FROM d_condutor
        WHERE nome = ? AND idade = ? AND certificacao = ?
    """, (nomecond, idade, certif))
    row = cur.fetchone()
    if row:
        return row[0]

    # criar novo id
    cur.execute("SELECT ISNULL(MAX(idcondutor),0) + 1 FROM d_condutor")
    new_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO d_condutor (idcondutor, nome, sexo, idade, certificacao)
        VALUES (?, ?, ?, ?, ?)
    """, (new_id, nomecond, sexo, idade, certif))

    # validação
    cur.execute("SELECT idcondutor FROM d_condutor WHERE idcondutor = ?", (new_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao obter idcondutor após INSERT em d_condutor.")
    return row[0]

def get_or_create_dim_localizacao(cur, cidade, pais):
    cur.execute("""
        SELECT idlocalizacao FROM d_localizacao
        WHERE cidade = ? AND pais = ?
    """, (cidade, pais))
    row = cur.fetchone()
    if row:
        return row[0]

    # gerar novo id
    cur.execute("SELECT ISNULL(MAX(idlocalizacao),0) + 1 FROM d_localizacao")
    new_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO d_localizacao (idlocalizacao, cidade, pais)
        VALUES (?, ?, ?)
    """, (new_id, cidade, pais))

    # validação
    cur.execute("SELECT idlocalizacao FROM d_localizacao WHERE idlocalizacao = ?", (new_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao obter idlocalizacao após INSERT em d_localizacao.")
    return new_id

def get_or_create_dim_tempo(cur, data):
    cur.execute("SELECT idtempo FROM d_tempo WHERE datacompleta = ?", (data,))
    row = cur.fetchone()
    if row:
        return row[0]

    dia = data.day
    mes = data.month
    ano = data.year
    trimestre = (mes - 1) // 3 + 1
    semestre  = 1 if mes <= 6 else 2

    cur.execute("""
        INSERT INTO d_tempo(datacompleta, dia, mes, ano, trimestre, semestre)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data, dia, mes, ano, trimestre, semestre))

    # validação
    cur.execute("SELECT idtempo FROM d_tempo WHERE datacompleta = ?", (data,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao obter idtempo após INSERT em d_tempo.")
    return row[0]

# --------------------------------------------

def main():

    print("1 - Ligação ao MsSQL")
    conn = get_mssql_conn()
    cur = conn.cursor()

    try:
        print(f"2 - A abrir ficheiro CSV: {CSV_PATH}")
        with open(CSV_PATH, mode="r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            linhas = 0

            for row in reader:
                idv = int(row["idviagem"])

                # Se já existe na FACT → ignorar
                cur.execute("SELECT idviagem FROM f_viagem WHERE idviagem = ?", (idv,))
                if cur.fetchone():
                    continue

                # ----------------------------------------------------------
                # Converter datas
                # ----------------------------------------------------------
                data_partida = datetime.strptime(row["datapartida"], "%d/%m/%Y").date()
                data_chegada = datetime.strptime(row["datachegada"], "%d/%m/%Y").date()

                # ----------------------------------------------------------
                # Métricas factuais
                # ----------------------------------------------------------
                receita_eur = float(row["taxa"].replace(",", ".")) * 0.86
                num_cont    = int(row["numerocontentores"])
                peso_tot    = float(row["peso"].replace(",", "."))
                capacidadeteu = int(row["capacidadeteu"])
                teu_total   = capacidadeteu  

                estado_mar = None # = map_estado_mar_to_int(row["estado_mar"])
                distancia_km = None # = float(row["distancia_km"].replace(",", "."))

                duracao_h = (
                    datetime.combine(data_chegada, datetime.min.time())
                    - datetime.combine(data_partida, datetime.min.time())
                ).days * 24

                # ----------------------------------------------------------
                # DIMENSÕES (via CSV)
                # ----------------------------------------------------------
                id_barco = get_or_create_dim_barco(
                    cur,
                    row["nomebarco"],
                    row["tipobarco"],
                    capacidadeteu,
                    row["pais_origem"] # interpretou-se que o pais do barco é o pais de origem da viagem
                )

                id_cond = get_or_create_dim_condutor(
                    cur,
                    row["nomecondutor"],
                    int(row["idadecondutor"]),
                    row["certificacao"],
                    row["sexo"]
                )

                id_loc = get_or_create_dim_localizacao(
                    cur,
                    row["cidade_origem"],
                    row["pais_origem"]
                )

                id_tpart = get_or_create_dim_tempo(cur, data_partida)
                id_tcheg = get_or_create_dim_tempo(cur, data_chegada)

                # --- FACT COMPLETA ---
                cur.execute("""
                    INSERT INTO f_viagem (
                        idviagem,
                        d_barco_idbarco,
                        d_condutor_idcondutor,
                        d_localizacao_idlocalizacao,
                        d_tempo_partida,
                        d_tempo_chegada,
                        receita_taxas_eur,
                        num_contentores,
                        peso_total_contentores_kg,
                        teu_total,
                        duracao_viagem_horas,
                        estado_mar,
                        distancia_km
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    idv,
                    id_barco,
                    id_cond,
                    id_loc,
                    id_tpart,
                    id_tcheg,
                    receita_eur,
                    num_cont,
                    peso_tot,
                    teu_total,
                    duracao_h,
                    estado_mar,
                    distancia_km
                ))

                linhas += 1
                if linhas % 100 == 0:
                    conn.commit()
                    print(f"{linhas} linhas processadas...")

        conn.commit()
        print(f"ETL concluído com sucesso! Linhas processadas: {linhas}")

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
