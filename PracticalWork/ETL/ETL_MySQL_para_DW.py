
import os
import sys
import mysql.connector as mysql
import pyodbc
from datetime import datetime
import random

# Variaveis de ligação aos SGBD
# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1") # ip da maquina onde está o mysql (localhost)
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306")) #porta do mysql (só mudar se alterou na instalação)
MYSQL_USER = os.getenv("MYSQL_USER", "root") # introduzir user
MYSQL_PWD  = os.getenv("MYSQL_PWD",  "1234") # introduzir password
MYSQL_DB   = os.getenv("MYSQL_DB",   "tp_g2") # introduzir nome da bd operacional

# SQL Server
MSSQL_HOST   = os.getenv("MSSQL_HOST", "127.0.0.1") # ip da maquina onde está o mysql (localhost)
MSSQL_PORT   = int(os.getenv("MSSQL_PORT", "1433")) #porta do mssql (só mudar se alterou na instalação)
MSSQL_USER   = os.getenv("MSSQL_USER", "sa") # introduzir user
MSSQL_PWD    = os.getenv("MSSQL_PWD",  "1234")  # introduzir password
MSSQL_DB     = os.getenv("MSSQL_DB",   "TP") # introduzir nome da bd dw
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")  # o driver que foi instalado

# ligação ao SGBD mysql
def get_mysql_conn():
    """Liga ao MySQL via mysql-connector-python."""
    return mysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PWD,
        database=MYSQL_DB, autocommit=True, charset="utf8mb4"
    )

# ligação ao SGBD MSSQL
def get_mssql_conn():
    """Liga ao SQL Server via pyodbc (schema padrão: dbo)."""
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DB};UID={MSSQL_USER};PWD={MSSQL_PWD};"
        f"TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str)


# Funcoes auxiliares

# para adicionar dados à dimensao: d_condutor
def gerar_sexo_aleatorio():
    """Devolve 'M' ou 'F' com probabilidade de 50% cada."""
    return 'M' if random.random() < 0.5 else 'F'

"""
# para adicionar o nivel do mar na tabela de factos aleatoriamente
def gerar_estado_mar_aleatorio():
    
    # Devolve um nível de estado do mar entre 1 e 5
    # com probabilidade igual para cada valor (20% cada). Tal como no mockaroo.
    
    return random.choice([1, 2, 3, 4, 5])

# para adicionar a distancia em km na tabela de factos aleatoriamente
def gerar_distancia_km_aleatoria():
    
    # Devolve uma distância entre 50 km e 4000 km (float). Tal como no mockaroo.
    
    return round(random.uniform(50, 4000), 1)
"""

# --------------- FUNÇÕES AUXILIARES DIMENSÕES ---------------

# Todas as funções fazemm 1) SELECT pela chave primaria, se não existir, 2) INSERT e por fim  3) SELECT outra vez para obter o Id para confirmar

def get_or_create_dim_barco(cur, r):
    cur.execute("SELECT idbarco FROM d_barco WHERE idbarco = ?", (r["idbarco"],))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO d_barco (idbarco, nome, tipo, tamanho, pais, capacidadeteu, nomeempresa)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        r["idbarco"], r["nomebarco"], r["tipobarco"], r["tamanhobarco"],
        r["paisbarco"], r["capacidadeteu"], r["nomeempresabarco"]
    ))

    cur.execute("SELECT idbarco FROM d_barco WHERE idbarco = ?", (r["idbarco"],))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao inserir barco!")
    return row[0]

def get_or_create_dim_condutor(cur, r):
    cur.execute("SELECT idcondutor FROM d_condutor WHERE idcondutor = ?", (r["idcondutor"],))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO d_condutor (idcondutor, nome, sexo, idade, certificacao)
        VALUES (?, ?, ?, ?, ?)
    """, (
        r["idcondutor"], r["nomecondutor"], gerar_sexo_aleatorio(),
        r["idadecondutor"], r["certificacao"]
    ))

    cur.execute("SELECT idcondutor FROM d_condutor WHERE idcondutor = ?", (r["idcondutor"],))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao inserir condutor!")
    return row[0]

def get_or_create_dim_localizacao(cur, r):
    cur.execute("SELECT idlocalizacao FROM d_localizacao WHERE idlocalizacao = ?", (r["id_origem"],))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO d_localizacao (idlocalizacao, cidade, pais)
        VALUES (?, ?, ?)
    """, (r["id_origem"], r["cidade_origem"], r["pais_origem"]))

    cur.execute("SELECT idlocalizacao FROM d_localizacao WHERE idlocalizacao = ?", (r["id_origem"],))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao inserir localização!")
    return row[0]

def get_or_create_dim_tempo(cur, data):
    cur.execute("SELECT idtempo FROM d_tempo WHERE datacompleta = ?", (data,))
    row = cur.fetchone()
    if row:
        return row[0]

    dia = data.day
    mes = data.month
    ano = data.year
    trimestre = (mes - 1) // 3 + 1
    semestre = 1 if mes <= 6 else 2

    cur.execute("""
        INSERT INTO d_tempo(datacompleta, dia, mes, ano, trimestre, semestre)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data, dia, mes, ano, trimestre, semestre))

    cur.execute("SELECT idtempo FROM d_tempo WHERE datacompleta = ?", (data,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Falha ao obter idtempo após INSERT em d_tempo.")
    return row[0]

# --------------------------------------------

def main():
    print("1 - Ligação ao MySQL")
    mysql_conn  = get_mysql_conn() # ligação ao mysql
    print("2 - Ligação ao MsSQL")
    mssql_conn = get_mssql_conn() 

    try:
        mysql_cur = mysql_conn.cursor(dictionary=True)
        mssql_cur = mssql_conn.cursor()

        # Apenas viagens concluídas
        print("3 - A ler viagens concluídas com destino 'figfoz'...")
        mysql_cur.execute("""
            SELECT
                v.idviagem,
                v.datapartida,
                v.datachegada,
                v.localizacao_idlocalizacao       AS id_origem,
                lo.cidade                         AS cidade_origem,
                lo.pais                           AS pais_origem,
                v.localizacao_idlocalizacao1      AS id_destino,
                ld.cidade                         AS cidade_destino,
                ld.pais                           AS pais_destino,
                v.barco_idbarco                   AS idbarco,
                b.nomebarco,
                b.tamanhobarco,
                b.paisbarco,
                b.tipobarco,
                b.capacidadeteu,
                e.nomeempresabarco,
                v.condutor_idcondutor             AS idcondutor,
                c.nomecondutor,
                c.idadecondutor,
                c.certificacao,
                COUNT(DISTINCT ct.idcontentor)    AS num_contentores,
                COALESCE(SUM(ct.pesocontentor),0) AS peso_total_contentores_kg,
                COALESCE(SUM(tx.valor),0)         AS receita_taxas_eur,
                TIMESTAMPDIFF(
                    HOUR, v.datapartida, v.datachegada
                )                                 AS duracao_viagem_horas
            FROM viagem v
            JOIN localizacao lo ON lo.idlocalizacao  = v.localizacao_idlocalizacao
            JOIN localizacao ld ON ld.idlocalizacao  = v.localizacao_idlocalizacao1
            JOIN barco b        ON b.idbarco         = v.barco_idbarco
            JOIN empresabarco e ON e.idempresabarco  = b.empresabarco_idempresabarco
            JOIN condutor c     ON c.idcondutor      = v.condutor_idcondutor
            LEFT JOIN contentores ct ON ct.viagem_idviagem = v.idviagem
            LEFT JOIN taxas tx       ON tx.viagem_idviagem = v.idviagem
            WHERE v.status = 'concluida'
            AND LOWER(ld.cidade) = 'figfoz'
            GROUP BY
                v.idviagem, v.datapartida, v.datachegada,
                v.localizacao_idlocalizacao, lo.cidade, lo.pais,
                v.localizacao_idlocalizacao1, ld.cidade, ld.pais,
                v.barco_idbarco, b.nomebarco, b.tamanhobarco, b.paisbarco,
                b.tipobarco, b.capacidadeteu, e.nomeempresabarco,
                v.condutor_idcondutor, c.nomecondutor, c.idadecondutor, c.certificacao;
        """)
    
        rows = mysql_cur.fetchall()
        print(f"   → {len(rows)} viagens lidas.")
    
        for idx, r in enumerate(rows, start=1):
            idv = r["idviagem"]

            # Se já existir na fact, não insere
            mssql_cur.execute("SELECT idviagem FROM f_viagem WHERE idviagem = ?", (idv,))
            if mssql_cur.fetchone():
                continue

            # --- Pega nos Ids das dimensões
            id_barco = get_or_create_dim_barco(mssql_cur, r)
            id_cond = get_or_create_dim_condutor(mssql_cur, r)
            id_loc = get_or_create_dim_localizacao(mssql_cur, r)
            id_tpart = get_or_create_dim_tempo(mssql_cur, r["datapartida"])
            id_tcheg = get_or_create_dim_tempo(mssql_cur, r["datachegada"])

            # Métricas da tabela de factos (para nao ficar incompleto)
            receita   = float(r["receita_taxas_eur"] or 0)
            num_cont  = int(r["num_contentores"] or 0)
            peso_tot  = float(r["peso_total_contentores_kg"] or 0)
            teu_total = int(r["capacidadeteu"] or 0) 
            duracao_h = float(r["duracao_viagem_horas"] or 0)

            estado_mar   = None # = gerar_estado_mar_aleatorio()
            distancia_km = None # = gerar_distancia_km_aleatoria()

            # --- FACT COMPLETA ---
            mssql_cur.execute("""
                INSERT INTO f_viagem(
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
                receita,
                num_cont,
                peso_tot,
                teu_total,
                duracao_h,
                estado_mar,
                distancia_km
            ))
    
            if idx % 500 == 0:
                print(f"... {idx} processadas")
                mssql_conn.commit()
    
        mssql_conn.commit()
        print(f"ETL concluido. Registos processados: {len(rows)}")
    finally:
        mysql_conn.close()
        mssql_conn.close()

if __name__ == "__main__":
    main()
