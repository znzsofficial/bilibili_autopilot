"""Check the final allowlisted ZIP with real SDK imports, without activation."""

from pathlib import Path
import importlib.util
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release", ROOT / "scripts/validate_release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
SMOKE = """import importlib,pathlib,pkgutil,sys,types
from maibot_sdk.components import collect_components
root=pathlib.Path(sys.argv[1]).resolve()
pkg=types.ModuleType('packed_bilibili');pkg.__path__=[str(root)];sys.modules[pkg.__name__]=pkg
module=importlib.import_module('packed_bilibili.plugin')
plugin=module.create_plugin()
assert not plugin.config_model().ecosystem.enabled
assert not plugin.config_model().comment.publish_enabled
assert any(c['name']=='discovery_v1' for c in collect_components(plugin))
for item in pkgutil.iter_modules([str(root)]):
    if item.name not in {'tests','scripts'}:
        imported=importlib.import_module('packed_bilibili.'+item.name)
        assert pathlib.Path(imported.__file__).resolve().is_relative_to(root)
assert not (root/'config.toml').exists()
print('Final archive real SDK discovery/import OK; no host/model/QQ activation')
"""


def main():
    release.validate(ROOT, None)
    with tempfile.TemporaryDirectory() as directory:
        archive = Path(directory) / "plugin.zip"
        with zipfile.ZipFile(archive, "w") as z:
            for name in release.FILES:
                z.writestr(name, (ROOT / name).read_bytes())
        with zipfile.ZipFile(archive) as z:
            assert set(z.namelist()) == set(release.FILES) and not z.testzip()
            z.extractall(Path(directory) / "unpacked")
        result = subprocess.run(
            [sys.executable, "-I", "-c", SMOKE, str(Path(directory) / "unpacked")],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=45,
        )
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        print(result.stdout.strip())


if __name__ == "__main__":
    main()
