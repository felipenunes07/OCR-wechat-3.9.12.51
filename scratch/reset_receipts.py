import sqlite3

def main():
    conn = sqlite3.connect('wechat_receipt_state.db')
    cur = conn.cursor()
    
    file_ids = (
        '581a72db40bf3865190011ad5c687bde74d04d51',
        'c2833b811781e75c362ed907ed6d4abc4d86f552'
    )
    
    cur.execute("""
        UPDATE receipts 
        SET sheet_status = 'SINK_PENDING', 
            sheet_committed_at = NULL, 
            excel_sheet = NULL, 
            excel_row = NULL 
        WHERE file_id IN (?, ?)
    """, file_ids)
    
    conn.commit()
    print(f"Reset {cur.rowcount} rows in receipts database to SINK_PENDING successfully.")
    conn.close()

if __name__ == '__main__':
    main()
