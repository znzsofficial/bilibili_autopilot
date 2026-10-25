# Neko Bilibili maintenance fork

Upstream: [namchengwuqiu/bilibili_autopilot](https://github.com/namchengwuqiu/bilibili_autopilot),
starting from `bf362a6364421a7c6ac3fcf90ad62f47a227d9a1` (0.7.1). Original MIT LICENSE,
author attribution and history remain intact. Fork contributions: NekoLaska.

- Same `local.bilibili-autopilot` ID, runtime directory, credentials and original
  comment/reply/unknown ledgers. Replace the existing directory; never run two copies.
- Comments and reply following retain upstream behavior. Publication is independently
  controlled by `comment.publish_enabled`; migration never enables it.
- Optional `ecosystem.enabled` records automatic, successfully read, interesting
  videos without another model call. Manual preview is not an automatic read.
- Signed `discovery_v1@1` supplies optional metadata to Neko Proactive's existing
  trigger, limited to both sides' group grants. Private targets are not implicit.
- Handoffs are at most once per Bilibili UID/video/QQ session. `shown` means only
  prepared request context, not model adoption or QQ delivery. Unknowns never replay.
- Ecosystem mode suppresses direct post-comment QQ sharing, avoiding two broadcasters;
  Bilibili comments are not suppressed. There is no new timer or Life dependency.
- Evidence is a subtitle/AI-summary excerpt, not visual/audio observation or personal
  experience. No cross-plugin database/cookie reads, host or persona mutation.
- Credentials, pairing keys, SQLite, config.toml and runtime data never enter Git/ZIP.
- Inherited release workflow is archived. Exact-SHA dual-case CI and package import
  must pass before standard hot update; no auto deployment/release.

Local QA: `python scripts/run_tests.py`, `python scripts/check_archive.py`.
All fixtures use synthetic data/mocked IPC; no paid/model/QQ/Bilibili mutations.
