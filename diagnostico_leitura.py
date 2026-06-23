r"""Mostra o que o OCR realmente leu nas ultimas leituras.

Uso (de qualquer pasta):
    & "C:\Users\felip\Desktop\Wechat Automation\wechat-ocr-auto\.venv\Scripts\python.exe" "C:\Users\felip\Desktop\Wechat Automation\wechat-ocr-auto\diagnostico_leitura.py"
    ... acrescente 50 no final para filtrar valor lido = 50
"""
import os
import sqlite3
import sys

# Banco sempre ao lado deste script, nao importa de qual pasta voce rode.
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wechat_receipt_state.db")

filtro_valor = None
if len(sys.argv) > 1:
    try:
        filtro_valor = float(sys.argv[1])
    except ValueError:
        filtro_valor = None

if not os.path.exists(DB):
    print(f"Banco nao encontrado: {DB}")
    sys.exit(1)

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

rows = con.execute(
    """
    SELECT txn_date, txn_time, amount, amount_source, currency,
           ocr_conf, parse_conf, review_needed, ocr_text
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
          f"ocr_conf={r['ocr_conf']} | parse_conf={r['parse_conf']} | "
          f"review={r['review_needed']}")
    print("-" * 70)
    texto = (r["ocr_text"] or "").strip()
    print(texto[:800] if texto else "(sem texto de OCR salvo)")
    print()
    if mostrados >= 10:
        break

if mostrados == 0:
    print("Nenhuma linha encontrada com esse filtro.")
con.close()
