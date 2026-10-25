"""Two real plugins in an isolated Host Runner; no platform/model/QQ calls."""

import argparse
import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock


async def verify(args):
    source = Path(__file__).resolve().parents[1]
    with ExitStack() as cleanup:
        instance = Path(cleanup.enter_context(tempfile.TemporaryDirectory(prefix="bili-ecosystem-"))).resolve()
        cleanup.callback(os.chdir, Path.cwd())
        os.chdir(instance)
        os.environ.update(MAIBOT_INSTANCE_DIR=str(instance), MAIBOT_HOST_VERSION="1.3.15", MAIBOT_LOCALE="zh-CN")
        if args.extra_site:
            sys.path.insert(0, str(args.extra_site.resolve()))
        sys.path.insert(0, str(args.host_source.resolve()))
        from src.plugin_runtime.runner.runner_main import PluginRunner, PluginActivationStatus
        from src.plugin_runtime.runner import runner_main
        from src.plugin_runtime.protocol.envelope import Envelope
        from src.common.database.database import engine
        from src.common.logger import close_handlers

        cleanup.callback(engine.dispose)
        cleanup.callback(close_handlers)
        assert runner_main._PROJECT_ROOT.resolve() == instance
        plugins = instance / "plugins"
        for name, root, files in [
            (
                "bilibili",
                source,
                [
                    "__init__.py",
                    "_manifest.json",
                    "plugin.py",
                    "config.py",
                    "engine.py",
                    "models.py",
                    "client.py",
                    "state.py",
                    "sharing.py",
                    "reading_bridge.py",
                    "webui.py",
                ],
            ),
            (
                "proactive",
                args.proactive_source,
                [
                    "_manifest.json",
                    "plugin.py",
                    "config.py",
                    "life_bridge.py",
                    "bilibili_bridge.py",
                    "prompt_visibility.py",
                ],
            ),
        ]:
            target = plugins / name
            target.mkdir(parents=True)
            for f in files:
                shutil.copyfile(root / f, target / f)
        (plugins / "bilibili/config.toml").write_text(
            '[plugin]\nconfig_version="0.2.0"\nenabled=true\n[browse]\nauto_start=false\n[ecosystem]\nenabled=true\n[share]\nqq_group_ids=["123456"]\n',
            encoding="utf-8",
        )
        (plugins / "proactive/config.toml").write_text(
            '[plugin]\nconfig_version="2.0.0"\nenabled=true\n[scheduler]\nenabled=false\n[target]\nallowed_groups=["123456"]\n[life]\nenabled=false\n[bilibili]\nenabled=true\n',
            encoding="utf-8",
        )
        runner = PluginRunner(
            host_address="unix:///unused-bili-test.sock", session_token="synthetic", plugin_dirs=[str(plugins)]
        )
        metas = {}
        triggers = []
        calls = []
        task = "proactive:neko.proactive:" + str(int(time.time() * 1000))

        async def rpc(method, **kwargs):
            if method != "cap.call":
                assert method in {"plugin.bootstrap", "plugin.register_components", "plugin.unregister"}, method
                return SimpleNamespace(error=None, payload={"accepted": True})
            payload = kwargs["payload"]
            capability, body = payload["capability"], payload["args"]
            calls.append(capability)
            if capability == "config.get":
                value = {"success": True, "value": body.get("default", "")}
            elif capability == "chat.get_group_streams":
                value = {
                    "success": True,
                    "streams": [
                        {
                            "session_id": "chat",
                            "group_id": "123456",
                            "account_id": "789",
                            "platform": "qq",
                            "chat_type": "group",
                        }
                    ],
                }
            elif capability == "api.call":
                assert body["api_name"] == "local.bilibili-autopilot.discovery_v1"
                request = Envelope(
                    request_id=5,
                    message_type="request",
                    method="plugin.invoke_api",
                    plugin_id="local.bilibili-autopilot",
                    payload={"component_name": "discovery_api", "args": body["args"]},
                )
                response = await runner._handle_invoke(request)
                assert response.error is None, response.error
                value = {"success": True, "result": response.payload["result"]}
            elif capability == "maisaka.proactive.trigger":
                triggers.append(body)
                value = {"success": True, "task_id": task}
            else:
                raise AssertionError("Forbidden external capability: " + capability)
            return SimpleNamespace(error=None, payload={"result": value})

        runner._rpc_client.send_request = AsyncMock(side_effect=rpc)
        found = runner._loader.discover_and_load([str(plugins)], extra_available={})
        assert not runner._loader.failed_plugins and len(found) == 2, runner._loader.failed_plugins
        for meta in found:
            assert runner._prepare_plugin_activation(meta) is None and not meta.config_error
            assert await runner._activate_plugin(meta) == PluginActivationStatus.LOADED
            metas[meta.manifest.id] = meta
        try:
            bili = metas["local.bilibili-autopilot"].instance
            proactive = metas["neko.proactive"].instance
            bili._scheduler.cancel()
            await asyncio.gather(bili._scheduler, return_exceptions=True)
            bili.client.uid = "1001"
            bili.engine.paused = False
            bili.config.plugin.enabled = True
            key = "ab" * 32
            for ctx, name in [(bili.ctx, "discovery-keys.json"), (proactive.ctx, "bilibili-client.json")]:
                p = Path(ctx.paths.data_dir) / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps({"neko.proactive": key}), encoding="utf-8")
                p.chmod(0o600)
            # Construct real source dataclasses from this loaded package.
            import importlib

            domain = importlib.import_module(type(bili).__module__.rsplit(".", 1)[0] + ".models")
            item = domain.VideoItem("BV1234567890", 123, "合成资料", "2", "UP", 90)
            evidence = domain.Evidence(
                "字幕", "这段字幕是本次隔离测试的合成来源，长度足够形成有依据的阅读资料，不涉及任何真实平台或账号。"
            )
            decision = domain.Decision(False, 0.8, "", "有趣但不必评论", "")
            await bili._remember_reading("1001", item, evidence, decision)
            pro_domain = importlib.import_module(type(proactive).__module__)
            state = pro_domain.ChatStreamState("chat", "123456", is_group=True)
            proactive._states = {(True, "123456"): state}
            assert await proactive._do_proactive_chat(state, "123456")
            assert len(triggers) == 1 and "合成来源" not in str(triggers)
            reason = triggers[0]["reason"]
            text = (
                f'<plugin_proactive_task id="{task}" plugin_id="neko.proactive">\n'
                f"插件请求你主动处理一轮聊天：群聊冷场了，主动找个话题聊聊\n触发原因：{reason}\n"
                "请结合当前聊天关系、记忆和上下文，自行决定是否回复以及如何表达。\n</plugin_proactive_task>"
            )
            hook_args = {
                "session_id": "chat",
                "items": [{"item_type": "UserMessageItem", "parts": [{"type": "text", "text": text}]}],
            }
            answer = await proactive.filter_planner_tasks(**hook_args)
            assert "合成来源" in str(answer), answer
            second = await proactive.filter_planner_tasks(**answer["modified_kwargs"])
            assert "合成来源" not in str(second)
            assert not bili.config.comment.publish_enabled
            result = {
                "real_host_runner_two_plugins": True,
                "real_sdk_signed_api_roundtrip": True,
                "body_absent_from_task_history": True,
                "request_only_material_once": True,
                "comments_remain_disabled": True,
                "life_not_used": True,
                "model_calls": 0,
                "bilibili_calls": 0,
                "qq_sends": 0,
                "capabilities": sorted(set(calls)),
            }
        finally:
            for meta in reversed(list(metas.values())):
                await runner._invoke_plugin_on_unload(meta)
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host-source", type=Path, required=True)
    p.add_argument("--proactive-source", type=Path, required=True)
    p.add_argument("--extra-site", type=Path)
    p.add_argument("--report", type=Path, required=True)
    args = p.parse_args()
    args.report = args.report.resolve()
    args.proactive_source = args.proactive_source.resolve()
    asyncio.run(verify(args))


if __name__ == "__main__":
    main()
