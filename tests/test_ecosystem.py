"""Only synthetic readings, fake SDK RPC, temp databases; no external effects."""

import asyncio
import hashlib
import hmac
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import AsyncMock

from maibot_sdk.context import PluginContext
from local_bilibili_autopilot.config import AutopilotConfig
from local_bilibili_autopilot.models import Decision, Evidence, VideoItem
from local_bilibili_autopilot.reading_bridge import ReadingBridge, ReadingStore, PROVIDER, CLIENT, io
import test_autopilot as upstream_tests

ITEM = VideoItem("BV1234567890", 123, "一条具体的视频", "2", "UP", 90)
EVIDENCE = Evidence(
    "字幕", "这是一段已实际读取的字幕来源，用于合成测试阅读资料，不涉及真实平台，也不会调用任何模型或发送消息。"
)
DECISION = Decision(False, 0.8, "", "感兴趣但没有必要发表评论", "")
TASK = "proactive:neko.proactive:1792930000000"


def packet(operation, body, key="ab" * 32):
    import uuid

    value = {
        "v": 1,
        "client": CLIENT,
        "audience": PROVIDER,
        "issued": int(time.time()),
        "nonce": uuid.uuid4().hex,
        "operation": operation,
        "body": body,
    }
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return {**value, "signature": hmac.new(bytes.fromhex(key), raw, hashlib.sha256).hexdigest()}


class AutomaticReadTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = upstream_tests.EngineTests.asyncSetUp

    async def test_non_comment_read_is_forwarded_with_publish_disabled_without_extra_generation(self):
        self.cfg.comment.publish_enabled = False
        self.generate.return_value = upstream_tests.decision(
            should_comment=False, interest=0.8, comment="", evidence_quote=""
        )
        sink = self.engine.reading_sink = AsyncMock()
        await self.engine.start()
        result = await self.engine.round()
        self.assertIn("看过不评论", result)
        sink.assert_awaited_once()
        self.generate.assert_awaited_once()
        self.client.publish.assert_not_awaited()

    async def test_manual_preview_never_produces_autonomous_material(self):
        sink = self.engine.reading_sink = AsyncMock()
        await self.engine.round(preview=True)
        sink.assert_not_awaited()

    async def test_sink_failure_does_not_cancel_comment_or_clear_comment_dedup(self):
        self.engine.reading_sink = AsyncMock(side_effect=OSError("synthetic storage failure"))
        await self.engine.start()
        result = await self.engine.round()
        self.assertIn("已提交评论", result)
        self.client.publish.assert_awaited_once()
        self.assertEqual(self.store.stats("1001", 0)["attempts"], 1)


class HandoffTests(unittest.IsolatedAsyncioTestCase):
    def test_config_copy_distinguishes_active_group_grant_from_legacy_share_options(self):
        schema = AutopilotConfig.model_json_schema()
        share = schema["$defs"]["ShareSection"]["properties"]
        self.assertIn("取交集", share["qq_group_ids"]["description"])
        self.assertIn("仍须保留本名单", share["qq_group_ids"]["hint"])
        self.assertIn("生态开启时本项不参与", share["polish_enabled"]["description"])
        self.assertIn("不是生态主动聊天超时", share["total_timeout_seconds"]["description"])
        cfg = AutopilotConfig()
        self.assertEqual(cfg.share.polish_timeout_seconds, 30)
        self.assertEqual(cfg.share.total_timeout_seconds, 120)
        self.assertEqual(cfg.ecosystem.daily_max, 2)
        self.assertEqual(cfg.ecosystem.interval_minutes, 180)
        self.assertFalse(cfg.ecosystem.enabled)
        self.assertFalse(cfg.comment.publish_enabled)

    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        key = self.root / "discovery-keys.json"
        key.write_text(json.dumps({CLIENT: "ab" * 32}), encoding="utf-8")
        key.chmod(0o600)
        self.cfg = AutopilotConfig()
        self.cfg.ecosystem.enabled = True
        self.cfg.share.qq_group_ids = ["123456"]
        self.current_uid = "1001"
        self.available = True
        self.streams = [
            {"session_id": "chat", "group_id": "123456", "account_id": "789", "platform": "qq", "chat_type": "group"}
        ]

        async def rpc(method, plugin_id, payload):
            self.assertEqual(payload["capability"], "chat.get_group_streams")
            return {"success": True, "streams": self.streams}

        self.ctx = PluginContext(PROVIDER, rpc_call=rpc)
        self.bridge = ReadingBridge(
            self.ctx, lambda: self.cfg, lambda: self.current_uid, lambda: self.available, self.root
        )
        self.bridge.store.remember("1001", ITEM, EVIDENCE, DECISION, time.time())

    async def claim(self, session="chat"):
        return await self.bridge.handle(packet("claim", {"session_id": session}))

    async def test_real_sdk_and_signed_handoff_prompt_is_once_and_not_delivery(self):
        self.assertFalse(self.cfg.comment.publish_enabled)
        claimed = await self.claim()
        self.assertTrue(claimed["success"])
        receipt = claimed["candidate"]["receipt_id"]
        body = {"session_id": "chat", "receipt_id": receipt, "host_task": TASK}
        first = await self.bridge.handle(packet("prompt", body))
        self.assertIn("实际读取的字幕", first["candidate"]["summary"])
        self.assertEqual((await self.bridge.handle(packet("prompt", body)))["candidate"], None)
        # Even a late submit acknowledgement cannot rewind the displayed state.
        ack = await self.bridge.handle(packet("receipt", {**body, "status": "submitted"}))
        self.assertEqual(ack["status"], "shown")
        denied = await self.bridge.handle(packet("receipt", {**body, "status": "sent"}))
        self.assertFalse(denied["success"])
        self.assertIsNone((await self.claim())["candidate"])

    async def test_signature_audience_nonce_replay_and_missing_key_fail_closed(self):
        p = packet("claim", {"session_id": "chat"})
        self.assertTrue((await self.bridge.handle(p))["success"])
        self.assertFalse((await self.bridge.handle(p))["success"])
        changed = packet("claim", {"session_id": "chat"})
        changed["audience"] = "neko.life"
        self.assertFalse((await self.bridge.handle(changed))["success"])
        (self.root / "discovery-keys.json").unlink()
        self.assertFalse((await self.claim())["success"])

    async def test_session_account_group_and_private_grants_do_not_leak(self):
        self.assertFalse((await self.claim("other"))["success"])
        self.streams[0]["chat_type"] = "private"
        self.assertFalse((await self.claim())["success"])
        self.streams[0]["chat_type"] = "group"
        self.streams.append({**self.streams[0], "session_id": "second", "account_id": "987"})
        self.assertFalse((await self.claim())["success"])
        self.cfg.ecosystem.account_id = "789"
        self.assertTrue((await self.claim())["success"])

    async def test_placeholder_route_is_not_a_second_robot_account(self):
        self.streams.append({**self.streams[0], "session_id": "placeholder", "account_id": None})
        self.assertTrue((await self.claim())["success"])

    async def test_revocation_closed_and_account_change_refuse_old_material(self):
        claimed = await self.claim()
        receipt = claimed["candidate"]["receipt_id"]
        self.cfg.ecosystem.enabled = False
        self.assertFalse(
            (
                await self.bridge.handle(
                    packet("prompt", {"session_id": "chat", "receipt_id": receipt, "host_task": TASK})
                )
            )["success"]
        )
        self.cfg.ecosystem.enabled = True
        self.current_uid = "9999"
        self.assertIsNone((await self.claim())["candidate"])
        self.bridge.closed = True
        self.assertFalse((await self.claim())["success"])

    async def test_restart_and_expiry_never_reoffer_same_video_or_clear_unknown(self):
        store = self.bridge.store
        now = time.time()
        first = store.claim("1001", "chat", "789", now, 2, 1)
        store.operate("receipt", "1001", "chat", "789", {"receipt_id": first["receipt_id"], "status": "unknown"}, now)
        reopened = ReadingStore(self.root)
        self.assertIsNone(reopened.claim("1001", "chat", "789", now + 1000, 2, 1))
        result = reopened.operate(
            "prompt", "1001", "chat", "789", {"receipt_id": first["receipt_id"], "host_task": TASK}, now + 1000
        )
        self.assertIsNone(result["candidate"])

    async def test_corrupt_database_is_preserved(self):
        self.bridge.store.path.write_bytes(b"not a sqlite database")
        before = self.bridge.store.path.read_bytes()
        self.assertFalse((await self.claim())["success"])
        self.assertEqual(self.bridge.store.path.read_bytes(), before)

    async def test_cancel_waits_for_database_worker_before_releasing_ownership(self):
        import threading

        started, release = threading.Event(), threading.Event()

        def work():
            started.set()
            release.wait(5)

        task = asyncio.create_task(io(work))
        while not started.is_set():
            await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        task.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
