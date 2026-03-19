#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import sqlite3
import pywxdump


def pick_group_name(remark: str | None, nickname: str | None, username: str) -> str:
    r = (remark or '').strip()
    n = (nickname or '').strip()
    if r:
        return r
    if n:
        return n
    return username


def main() -> int:
    base_dir = Path(r"C:\Users\admin\Desktop\WeChat_OCR_Auto")
    map_path = base_dir / "clientes_grupos.json"
    dec_dir = base_dir / "decrypted_msg"
    dec_dir.mkdir(parents=True, exist_ok=True)

    infos = pywxdump.get_wx_info(is_print=False)
    if not infos:
        print("ERR: WeChat nao encontrado em execucao")
        return 2
    info = infos[0]
    wx_dir = Path(info.get("wx_dir") or "")
    key = info.get("key") or ""
    if not wx_dir.exists() or not key:
        print("ERR: nao foi possivel obter wx_dir/key")
        return 3

    src_micro = wx_dir / "Msg" / "MicroMsg.db"
    src_room = wx_dir / "Msg" / "ChatRoomUser.db"
    pywxdump.batch_decrypt(key=key, db_path=[str(src_micro), str(src_room)], out_path=str(dec_dir), is_print=False)

    de_micro = dec_dir / "de_MicroMsg.db"
    if not de_micro.exists():
        print("ERR: de_MicroMsg.db nao gerado")
        return 4

    msgattach_dir = wx_dir / "FileStorage" / "MsgAttach"
    folder_hashes = {p.name.lower() for p in msgattach_dir.iterdir() if p.is_dir()} if msgattach_dir.exists() else set()

    conn = sqlite3.connect(de_micro)
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT UserName, NickName, Remark FROM Contact WHERE UserName LIKE '%@chatroom'"
    ).fetchall()
    conn.close()

    auto_map: dict[str, str] = {}
    for username, nickname, remark in rows:
        if not username:
            continue
        h = hashlib.md5(username.encode("utf-8")).hexdigest()
        if folder_hashes and h not in folder_hashes:
            continue
        auto_map[h] = pick_group_name(remark, nickname, username)

    existing: dict[str, str] = {}
    if map_path.exists():
        try:
            data = json.loads(map_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for k, v in data.items():
                    existing[str(k).strip().lower()] = str(v).strip()
        except Exception:
            existing = {}

    # Fill only blanks; keep manual names untouched
    changed = 0
    for h, name in auto_map.items():
        old = existing.get(h, "")
        if not old:
            existing[h] = name
            changed += 1

    for h in sorted(folder_hashes):
        existing.setdefault(h, "")

    existing.pop("cole_aqui_id_do_grupo", None)
    map_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

    filled = sum(1 for v in existing.values() if str(v).strip())
    print(f"OK: mapa atualizado | total_hash={len(existing)} | preenchidos={filled} | novos={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
