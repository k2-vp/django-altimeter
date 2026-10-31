"""Verify the versioned browser sources from built distribution artifacts."""

import email
import json
import subprocess
import sys
import tarfile
from pathlib import Path
from zipfile import ZipFile

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
BROWSER = "django_altimeter/browser/"
REQUIRED = {
    "headers.ts",
    "journey.ts",
    "resource_timing.ts",
    "VERSION",
    "examples/asgi.ts",
    "examples/wsgi.ts",
}


def _artifact(pattern: str) -> Path:
    artifacts = list((ROOT / "dist").glob(pattern))
    assert len(artifacts) == 1, f"Build exactly one {pattern} with uv build"
    return artifacts[0]


def test_browser_sources_and_version_ship_in_wheel_and_sdist() -> None:
    with ZipFile(_artifact("django_altimeter-*.whl")) as wheel:
        files = {
            name.removeprefix(BROWSER): wheel.read(name)
            for name in wheel.namelist()
            if name.startswith(BROWSER)
        }
        assert REQUIRED <= files.keys(), "Missing vendorable browser source assets"
        metadata_names = [
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        ]
        assert len(metadata_names) == 1
        version = email.message_from_bytes(wheel.read(metadata_names[0]))["Version"]
        assert files["VERSION"].decode("ascii").strip() == version
        assert all(data and len(data) < 100_000 for data in files.values())
        assert all(name == "VERSION" or name.endswith((".ts", ".md")) for name in files)
    with tarfile.open(_artifact("django_altimeter-*.tar.gz")) as sdist:
        prefix = f"django_altimeter-{version}/src/{BROWSER}"
        source_files = {}
        for member in sdist.getmembers():
            if member.isfile() and member.name.startswith(prefix):
                resource = sdist.extractfile(member)
                assert resource is not None
                source_files[member.name.removeprefix(prefix)] = resource.read()
        assert source_files == files, "Wheel and sdist browser sources differ"


def _run(command: list[str], directory: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command, cwd=directory, capture_output=True, text=True, timeout=120, check=False
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    return result


def test_installed_wheel_browser_assets_are_readable_outside_checkout(
    tmp_path: Path,
) -> None:
    environment = tmp_path / "venv"
    _run([sys.executable, "-m", "venv", str(environment)], tmp_path)
    python = environment / "bin/python"
    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            str(_artifact("django_altimeter-*.whl")),
        ],
        tmp_path,
    )
    result = _run(
        [
            str(python),
            "-I",
            "-c",
            """
import importlib.metadata
import importlib.resources
import json
import sys
from pathlib import Path

package = importlib.resources.files('django_altimeter')
assert Path(str(package)).is_relative_to(Path(sys.prefix))
browser = package.joinpath('browser')
version = browser.joinpath('VERSION').read_text(encoding='ascii').strip()
assert version == importlib.metadata.version('django-altimeter')
for name in ('journey.ts', 'headers.ts'):
    assert browser.joinpath(name).read_text(encoding='utf-8').strip()
print(json.dumps({'version': version, 'browser': str(browser)}))
""",
        ],
        tmp_path,
    )
    report = json.loads(result.stdout)
    assert report["version"] == _artifact("django_altimeter-*.whl").name.split("-")[1]
    assert Path(report["browser"]).is_relative_to(environment)
