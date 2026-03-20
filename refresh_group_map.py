#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any


def stable_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def pick_group_name(
    session_name: str | None,
    remark: str | None,
    nickname: str | None,
    username: str,
) -> str:
    r = (remark or "").strip()
    s = (session_name or "").strip()
    if r:
        return r
    if s:
        return s
    n = (nickname or "").strip()
    if n:
        return n
    return username


def load_existing_map(map_path: Path) -> dict[str, str]:
    existing: dict[str, str] = {}
    if not map_path.exists():
        return existing
    try:
        data = json.loads(map_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k, v in data.items():
                existing[str(k).strip().lower()] = str(v).strip()
    except Exception:
        existing = {}
    return existing


def discover_document_wechat_dirs() -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()

    for home_raw in [
        os.environ.get("USERPROFILE"),
        str(Path.home()),
    ]:
        if not home_raw:
            continue
        home = Path(home_raw)
        for root in [
            home / "Documents" / "WeChat Files",
            home / "OneDrive" / "Documents" / "WeChat Files",
        ]:
            key = str(root).lower()
            if key in seen:
                continue
            seen.add(key)
            if root.exists() and root.is_dir():
                roots.append(root)

    wx_dirs: list[Path] = []
    for root in roots:
        try:
            account_dirs = list(root.iterdir())
        except OSError:
            continue
        for account_dir in account_dirs:
            if not account_dir.is_dir():
                continue
            fs_dir = account_dir / "FileStorage"
            if fs_dir.exists() and fs_dir.is_dir():
                wx_dirs.append(account_dir)

    def _safe_mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    wx_dirs.sort(key=_safe_mtime, reverse=True)
    return wx_dirs


def collect_msgattach_hash_folders(wx_dirs: list[Path]) -> set[str]:
    folder_hashes: set[str] = set()
    for wx_dir in wx_dirs:
        msgattach_dir = wx_dir / "FileStorage" / "MsgAttach"
        if not msgattach_dir.exists() or not msgattach_dir.is_dir():
            continue
        for entry in msgattach_dir.iterdir():
            if entry.is_dir():
                folder_hashes.add(entry.name.strip().lower())
    return folder_hashes


def pick_pywxdump_target(infos: list[dict[str, Any]]) -> tuple[Path | None, str]:
    for info in infos:
        wx_dir_raw = str(info.get("wx_dir") or "").strip()
        key = str(info.get("key") or "").strip()
        if not wx_dir_raw:
            continue
        wx_dir = Path(wx_dir_raw)
        if wx_dir.exists() and key:
            return wx_dir, key
    return None, ""


def build_auto_map_from_db(
    wx_dir: Path,
    key: str,
    dec_dir: Path,
    pywxdump: Any,
) -> tuple[dict[str, str], set[str]]:
    src_micro = wx_dir / "Msg" / "MicroMsg.db"
    src_room = wx_dir / "Msg" / "ChatRoomUser.db"
    if not src_micro.exists() or not src_room.exists():
        raise FileNotFoundError("MicroMsg.db or ChatRoomUser.db not found")

    pywxdump.batch_decrypt(
        key=key,
        db_path=[str(src_micro), str(src_room)],
        out_path=str(dec_dir),
        is_print=False,
    )

    de_micro = dec_dir / "de_MicroMsg.db"
    if not de_micro.exists():
        raise RuntimeError("de_MicroMsg.db not generated")

    conn = sqlite3.connect(de_micro)
    cur = conn.cursor()
    contact_rows = cur.execute(
        "SELECT UserName, NickName, Remark FROM Contact WHERE UserName LIKE '%@chatroom'"
    ).fetchall()
    session_rows = cur.execute(
        "SELECT strUsrName, strNickName, nTime FROM Session WHERE strUsrName LIKE '%@chatroom'"
    ).fetchall()
    non_group_usernames = {
        username
        for (username,) in cur.execute(
            "SELECT UserName FROM Contact WHERE UserName NOT LIKE '%@chatroom' AND UserName != ''"
        ).fetchall()
        if username
    }
    non_group_usernames.update(
        username
        for (username,) in cur.execute(
            "SELECT strUsrName FROM Session WHERE strUsrName NOT LIKE '%@chatroom' AND strUsrName != ''"
        ).fetchall()
        if username
    )
    conn.close()

    session_name_by_username: dict[str, tuple[int, str]] = {}
    for username, session_name, ntime in session_rows:
        if not username:
            continue
        ts = int(ntime or 0)
        old = session_name_by_username.get(username)
        if old is None or ts >= old[0]:
            session_name_by_username[username] = (ts, (session_name or "").strip())

    auto_map: dict[str, str] = {}
    for username, nickname, remark in contact_rows:
        if not username:
            continue
        h = stable_hash(username)
        session_name = session_name_by_username.get(username, (0, ""))[1]
        auto_map[h] = pick_group_name(session_name, remark, nickname, username)

    non_group_hashes = {stable_hash(username).lower() for username in non_group_usernames}
    return auto_map, non_group_hashes


def main() -> int:
    # Keep all generated artifacts inside the project folder so it works on any PC/user.
    base_dir = Path(__file__).resolve().parent
    map_path = base_dir / "clientes_grupos.json"
    dec_dir = base_dir / "decrypted_msg"
    dec_dir.mkdir(parents=True, exist_ok=True)

    existing = load_existing_map(map_path)
    auto_map: dict[str, str] = {}
    non_group_hashes: set[str] = set()
    folder_hashes: set[str] = set()
    notes: list[str] = []

    pywxdump: Any = None
    try:
        import pywxdump as _pywxdump  # type: ignore

        pywxdump = _pywxdump
        # Hide noisy unsupported-version warning from pywxdump; real errors still appear.
        logging.getLogger("wx_core").setLevel(logging.ERROR)
    except Exception as exc:
        notes.append(f"pywxdump_unavailable:{type(exc).__name__}:{exc}")

    if pywxdump is not None:
        try:
            infos = pywxdump.get_wx_info(is_print=False) or []
        except Exception as exc:
            infos = []
            notes.append(f"wx_info_failed:{type(exc).__name__}:{exc}")

        wx_dir, key = pick_pywxdump_target(infos)
        if wx_dir is None:
            notes.append("wx_info_missing_usable_key_or_dir")
        else:
            folder_hashes.update(collect_msgattach_hash_folders([wx_dir]))
            try:
                auto_map, non_group_hashes = build_auto_map_from_db(wx_dir=wx_dir, key=key, dec_dir=dec_dir, pywxdump=pywxdump)
            except Exception as exc:
                notes.append(f"decrypt_map_failed:{type(exc).__name__}:{exc}")

    if not folder_hashes:
        fallback_wx_dirs = discover_document_wechat_dirs()
        folder_hashes.update(collect_msgattach_hash_folders(fallback_wx_dirs))
        if fallback_wx_dirs:
            notes.append(f"fallback_hash_scan_dirs={len(fallback_wx_dirs)}")
        else:
            notes.append("fallback_hash_scan_no_wechat_files")

    # Fill only blanks; keep manual names untouched.
    changed = 0
    for h, name in auto_map.items():
        old = existing.get(h, "")
        if not old:
            existing[h] = name
            changed += 1

    for h in sorted(folder_hashes):
        if h in auto_map or h in non_group_hashes:
            continue
        existing.setdefault(h, "")

    existing.pop("cole_aqui_id_do_grupo", None)
    for h in non_group_hashes:
        if not existing.get(h, "").strip():
            existing.pop(h, None)

    map_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

    filled = sum(1 for v in existing.values() if str(v).strip())
    mode = "full" if auto_map else "hash-only"
    print(
        f"OK: mapa atualizado | modo={mode} | total_hash={len(existing)} | "
        f"preenchidos={filled} | novos={changed}"
    )
    for note in notes:
        print(f"WARN: {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
