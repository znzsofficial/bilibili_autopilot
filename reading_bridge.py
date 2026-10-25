"""Bounded read-only discoveries for Neko Proactive, not another sender/timer.

Each video/chat handoff is consumed at most once, even when delivery is unknown.
`submitted` and `shown` never mean that QQ actually received a message.
The original account/comment databases and credentials are not modified here.
"""

import asyncio
from contextlib import contextmanager
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

PROVIDER = "local.bilibili-autopilot"
CLIENT = "neko.proactive"


async def io(function, *args):
    job = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(job)
    except asyncio.CancelledError:
        while not job.done():
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                pass
            except Exception:
                break
        # Do not replace caller cancellation with a worker error.
        if job.done() and not job.cancelled():
            job.exception()
        raise


def authenticate(packet, directory, now):
    path = Path(directory) / "discovery-keys.json"
    if path.is_symlink() or path.stat().st_size > 1024 or (os.name == "posix" and path.stat().st_mode & 0o077):
        raise ValueError("unsafe_discovery_key")
    key = json.loads(path.read_text(encoding="utf-8")).get(CLIENT)
    if not isinstance(key, str) or not re.fullmatch("[a-f0-9]{64}", key):
        raise ValueError("invalid_discovery_key")
    fields = {"v", "client", "audience", "issued", "nonce", "operation", "body", "signature"}
    if not isinstance(packet, dict) or set(packet) != fields:
        raise ValueError("invalid_packet")
    if (
        type(packet["v"]) is not int
        or packet["v"] != 1
        or packet["client"] != CLIENT
        or packet["audience"] != PROVIDER
        or type(packet["issued"]) is not int
        or abs(now - packet["issued"]) > 120
        or not isinstance(packet["body"], dict)
        or not isinstance(packet["nonce"], str)
        or not re.fullmatch("[a-f0-9]{32}", packet["nonce"])
        or not isinstance(packet["signature"], str)
        or not re.fullmatch("[a-f0-9]{64}", packet["signature"])
    ):
        raise ValueError("invalid_packet")
    raw = json.dumps(
        {k: v for k, v in packet.items() if k != "signature"},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    if len(raw) > 12000 or not hmac.compare_digest(
        packet["signature"], hmac.new(bytes.fromhex(key), raw, hashlib.sha256).hexdigest()
    ):
        raise ValueError("invalid_signature")
    return packet["operation"], packet["body"], packet["nonce"]


class ReadingStore:
    def __init__(self, directory):
        self.path = Path(directory) / "discovery.sqlite3"

    @contextmanager
    def connect(self):
        if self.path.is_symlink():
            raise ValueError("unsafe_discovery_database")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=3)
        self.path.chmod(0o600)
        db.row_factory = sqlite3.Row
        try:
            with db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS readings(uid TEXT,bvid TEXT,created REAL,payload TEXT,PRIMARY KEY(uid,bvid))"
                )
                db.execute(
                    "CREATE TABLE IF NOT EXISTS offers(id TEXT PRIMARY KEY,uid TEXT,bvid TEXT,session TEXT,account TEXT,created REAL,status TEXT,task TEXT,shown INTEGER,UNIQUE(uid,bvid,session))"
                )
                db.execute("CREATE TABLE IF NOT EXISTS nonces(id TEXT PRIMARY KEY,created REAL)")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def remember(self, uid, item, evidence, decision, now):
        if not uid.isdecimal() or not re.fullmatch(r"BV[0-9A-Za-z]{10}", item.bvid):
            raise ValueError("invalid_reading_identity")
        text = evidence.text.strip()
        if len(text) < 30:
            return
        payload = {
            "topic": item.title[:160],
            "summary": text[:480],
            "opinion": decision.reason[:160],
            "url": "https://www.bilibili.com/video/" + item.bvid,
            "source_kind": evidence.source[:100],
            "coverage": evidence.coverage[:160],
            "fictional": False,
            "kind": "bilibili",
        }
        with self.connect() as db:
            db.execute("DELETE FROM readings WHERE created<?", (now - 172800,))
            db.execute(
                "INSERT OR IGNORE INTO readings VALUES(?,?,?,?)",
                (uid, item.bvid, now, json.dumps(payload, ensure_ascii=False)),
            )
            db.execute(
                "DELETE FROM readings WHERE rowid IN (SELECT rowid FROM readings ORDER BY created DESC LIMIT -1 OFFSET 256)"
            )

    def nonce(self, nonce, now):
        with self.connect() as db:
            db.execute("DELETE FROM nonces WHERE created<?", (now - 300,))
            if db.execute("SELECT COUNT(*) FROM nonces").fetchone()[0] >= 2048:
                raise ValueError("nonce_capacity")
            db.execute("INSERT INTO nonces VALUES(?,?)", (nonce, now))

    def claim(self, uid, session, account, now, daily_max, interval_minutes):
        # Expired/failed/unknown handoffs are tombstones, not a retry queue.
        with self.connect() as db:
            if db.execute("SELECT COUNT(*) FROM offers").fetchone()[0] >= 8192:
                raise ValueError("handoff_capacity")
            used = db.execute(
                "SELECT created FROM offers WHERE uid=? AND session=? AND status!='released' ORDER BY created DESC",
                (uid, session),
            ).fetchall()
            from datetime import datetime
            from zoneinfo import ZoneInfo

            start = (
                datetime.fromtimestamp(now, ZoneInfo("Asia/Shanghai"))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .timestamp()
            )
            if sum(r[0] >= start for r in used) >= daily_max or (used and now - used[0][0] < interval_minutes * 60):
                return None
            row = db.execute(
                "SELECT bvid,payload FROM readings r WHERE uid=? AND created>? AND NOT EXISTS (SELECT 1 FROM offers o WHERE o.uid=r.uid AND o.bvid=r.bvid AND o.session=?) ORDER BY created DESC LIMIT 1",
                (uid, now - 172800, session),
            ).fetchone()
            if row is None:
                return None
            receipt = uuid.uuid4().hex
            db.execute(
                "INSERT INTO offers VALUES(?,?,?,?,?,?,?,'',0)",
                (receipt, uid, row["bvid"], session, account, now, "prepared"),
            )
            return {"receipt_id": receipt, "protocol": 2, "summary": "一条已读B站资料，可在本次任务按需分享"}

    def operate(self, operation, uid, session, account, body, now):
        receipt = body.get("receipt_id")
        if not isinstance(receipt, str) or not re.fullmatch("[a-f0-9]{32}", receipt):
            raise ValueError("invalid_receipt")
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM offers WHERE id=? AND uid=? AND session=? AND account=?",
                (receipt, uid, session, account),
            ).fetchone()
            if row is None:
                raise ValueError("receipt_not_owned")
            if operation == "status":
                return {"success": True, "status": row["status"]}
            task = body.get("host_task", "")
            if not isinstance(task, str) or (task and not re.fullmatch(r"proactive:neko\.proactive:\d{10,16}", task)):
                raise ValueError("invalid_host_task")
            if row["task"] and task and row["task"] != task:
                raise ValueError("host_task_mismatch")
            if operation == "receipt":
                status = body.get("status")
                if status not in {"submitted", "released", "unknown"}:
                    raise ValueError("not_a_delivery_receipt")
                if row["shown"]:
                    return {"success": True, "status": "shown"}
                if row["status"] in {"released", "unknown"}:
                    return {"success": True, "status": row["status"]}
                if status == "submitted" and not task:
                    raise ValueError("missing_host_task")
                db.execute("UPDATE offers SET status=?,task=? WHERE id=?", (status, task or row["task"], receipt))
                return {"success": True, "status": status}
            if operation != "prompt" or not task:
                raise ValueError("invalid_operation")
            if row["shown"] or row["status"] in {"released", "unknown"} or not 0 <= now - row["created"] < 180:
                return {"success": True, "candidate": None}
            material = db.execute("SELECT payload FROM readings WHERE uid=? AND bvid=?", (uid, row["bvid"])).fetchone()
            db.execute("UPDATE offers SET shown=1,status='shown',task=? WHERE id=?", (task, receipt))
            return {
                "success": True,
                "candidate": {**json.loads(material[0]), "receipt_id": receipt} if material else None,
            }


class ReadingBridge:
    def __init__(self, context, get_config, get_uid, available, directory):
        self.context, self.get_config, self.get_uid, self.available = context, get_config, get_uid, available
        self.directory = Path(directory)
        self.store = ReadingStore(directory)
        self.closed = False
        self.pending = 0

    def policy(self):
        cfg = self.get_config()
        return (
            cfg.plugin.enabled,
            cfg.ecosystem.enabled,
            tuple(cfg.share.qq_group_ids),
            cfg.ecosystem.account_id,
            cfg.ecosystem.daily_max,
            cfg.ecosystem.interval_minutes,
            self.get_uid(),
        )

    async def handle(self, packet):
        if self.pending >= 8:
            return {"success": False, "error": "discovery_busy"}
        self.pending += 1
        try:
            snapshot = self.policy()
            if self.closed or not snapshot[0] or not snapshot[1] or not self.available():
                return {"success": False, "error": "discovery_disabled"}
            now = time.time()
            operation, body, nonce = await io(authenticate, packet, self.directory, now)
            session = body.get("session_id")
            if not isinstance(session, str) or not 0 < len(session) <= 128:
                raise ValueError("invalid_session")
            streams = await asyncio.wait_for(self.context.chat.get_group_streams(platform="qq"), timeout=2)
            if not isinstance(streams, list) or len(streams) > 512:
                raise ValueError("unavailable_qq_scopes")
            owned = [s for s in streams if isinstance(s, dict) and s.get("session_id") == session]
            if len(owned) != 1:
                raise ValueError("ambiguous_session")
            stream = owned[0]
            group, account = str(stream.get("group_id") or ""), str(stream.get("account_id") or "")
            matches = [
                s
                for s in streams
                if isinstance(s, dict)
                and str(s.get("group_id") or "") == group
                and s.get("platform") == "qq"
                and s.get("chat_type") == "group"
                and s.get("session_id")
                and str(s.get("account_id") or "").isdecimal()
                and (not snapshot[3] or str(s.get("account_id") or "") == snapshot[3])
            ]
            if (
                stream.get("platform") != "qq"
                or stream.get("chat_type") != "group"
                or group not in snapshot[2]
                or not account.isdecimal()
                or len(matches) != 1
                or (snapshot[3] and account != snapshot[3])
            ):
                raise ValueError("unauthorized_qq_scope")
            if self.closed or self.policy() != snapshot or not self.available():
                raise ValueError("discovery_changed")
            await io(self.store.nonce, nonce, now)
            if operation == "claim":
                value = {
                    "success": True,
                    "candidate": await io(
                        self.store.claim, snapshot[6], session, account, now, snapshot[4], snapshot[5]
                    ),
                }
            else:
                value = await io(self.store.operate, operation, snapshot[6], session, account, body, now)
            if self.closed or self.policy() != snapshot or not self.available():
                raise ValueError("discovery_changed")
            return value
        except asyncio.CancelledError:
            raise
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
            return {"success": False, "error": "discovery_unavailable_or_unauthorized"}
        finally:
            self.pending -= 1
