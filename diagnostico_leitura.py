r"""Diagnostico de leituras do OCR.

Uso (de qualquer pasta):
  Ver ultimas leituras:
    & "C:\Users\felip\Desktop\Wechat Automation\wechat-ocr-auto\.venv\Scripts\python.exe" "C:\Users\felip\Desktop\Wechat Automation\wechat-ocr-auto\diagnostico_leitura.py"
  Filtrar por valor lido (ex.: 18236120):
    ... diagnostico_leitura.py 18236120
  Listar linhas DUPLICADAS:
    ... diagnostico_leitura.py dups
"""
import os
import sqlite3
import sys

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wechat_receipt_state.db")

if not os.path.exists(DB):
    print(f"Banco nao encontrado: {DB}")
    sys.exit(1)

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

modo = sys.argv[1] if len(sys.argv) > 1 else None

if modo == "dups":
    # Agrupa por conteudo (cliente/data/hora/banco/valor) e mostra os repetidos.
    rows = con.execute(
        """
        SELECT client, txn_date, txn_time, bank, amount,
               COUNT(*) AS qtd,
               GROUP_CONCAT(file_id, ' | ') AS file_ids,
               GROUP_CONCAT(IFNULL(sha256,''), ' | ') AS shas,
               GROUP_CONCAT(IFNULL(msg_svr_id,'-'), ' | ') AS msgs,
               GROUP_CONCAT(IFNULL(sheet_status,''), ' | ') AS status
        FROM receipts
        GROUP BY client, txn_date, txn_time, bank, amount
        HAVING COUNT(*) > 1
        ORDER BY MAX(ingested_at) DESC
        LIMIT 30
        """
    ).fetchall()
    if not rows:
        print("Nenhuma duplicata encontrada.")
    for r in rows:
        print("=" * 70)
        print(f"{r['qtd']}x  cliente={r['client']} | {r['txn_date']} {r['txn_time']} | "
              f"{r['bank']} | valor={r['amount']}")
        print(f"  file_ids : {r['file_ids']}")
        print(f"  sha256   : {r['shas']}")
        print(f"  msg_id   : {r['msgs']}")
        print(f"  status   : {r['status']}")
    con.close()
    sys.exit(0)

filtro_valor = None
if modo is not None:
    try:
        filtro_valor = float(modo)
    except ValueError:
        filtro_valor = None

rows = con.execute(
    """
    SELECT txn_date, txn_time, amount, amount_source, currency,
           ocr_conf, parse_conf, review_needed, file_id, sha256, msg_svr_id,
           sheet_status, ocr_text
    FROM receipts
    ORDER BY ingested_at DESC
    LIMIT 60
    """
).fetchall()

mostrados = 0
for r in rows:
    if filtro_valor is not None and (r["amount"] is None or float(r["amount"]) != filtro_valor):
        continue
    mostrados += 1
    print("=" * 70)
    print(f"data={r['txn_date']} {r['txn_time']} | VALOR LIDO={r['amount']} "
          f"({r['currency']}) | origem={r['amount_source']} | "
          f"ocr_conf={r['ocr_conf']} | parse_conf={r['parse_conf']} | review={r['review_needed']}")
    print(f"file_id={r['file_id']} | sha256={r['sha256']} | msg_id={r['msg_svr_id']} | status={r['sheet_status']}")
    print("-" * 70)
    texto = (r["ocr_text"] or "").strip()
    print(texto[:900] if texto else "(sem texto de OCR salvo)")
    print()
    if mostrados >= 10:
        break

if mostrados == 0:
    print("Nenhuma linha encontrada com esse filtro.")
con.close()
