"""Check the artifacts from ``uv build`` without importing the source checkout."""

import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = PurePosixPath("django_altimeter")
BROWSER_ASSETS = {
    PACKAGE / "browser" / name
    for name in (
        "VERSION",
        "headers.ts",
        "journey.ts",
        "resource_timing.ts",
        "examples/asgi.ts",
        "examples/wsgi.ts",
    )
}
REQUIRED_ASSETS = {PACKAGE / "__init__.py", PACKAGE / "py.typed"} | BROWSER_ASSETS
FORBIDDEN_PARTS = {
    "tests",
    "reference",
    "plans",
    "design",
    ".beads",
    ".cass",
    ".ntm",
    ".bv",
}


def _artifact(pattern: str) -> Path:
    artifacts = list((ROOT / "dist").glob(pattern))
    assert len(artifacts) == 1, f"Build exactly one {pattern} with uv build"
    return artifacts[0]


def _is_package_asset(path: PurePosixPath) -> bool:
    return (
        path.is_relative_to(PACKAGE)
        and not FORBIDDEN_PARTS.intersection(path.parts)
        and (
            path.suffix == ".py"
            or path == PACKAGE / "py.typed"
            or path in BROWSER_ASSETS
        )
    )


def _run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True, timeout=180, check=False
    )
    assert result.returncode == 0, (
        f"{command!r} exited {result.returncode}\n{result.stdout}\n{result.stderr}"
    )
    return result


def test_wheel_contains_only_package_and_typing_assets() -> None:
    wheel = _artifact("django_altimeter-*.whl")
    with ZipFile(wheel) as archive:
        names = {PurePosixPath(name) for name in archive.namelist()}
        assert REQUIRED_ASSETS <= names
        metadata_root = f"django_altimeter-{wheel.name.split('-')[1]}.dist-info"
        metadata = {
            PurePosixPath(metadata_root) / name
            for name in (
                "METADATA",
                "WHEEL",
                "RECORD",
                "top_level.txt",
                "licenses/LICENSE",
            )
        }
        assert metadata <= names
        assert all(_is_package_asset(path) or path in metadata for path in names)
        assert all(not path.is_absolute() and ".." not in path.parts for path in names)

        # Images validate their own installed wheel as well as the clean venv below.
        if "ALTIMETER_REFERENCE_PYTHON" in os.environ:
            assert (
                f"{sys.version_info.major}.{sys.version_info.minor}"
                == os.environ["ALTIMETER_REFERENCE_PYTHON"]
            )
            assert (
                importlib.metadata.version("Django")
                == os.environ["ALTIMETER_REFERENCE_DJANGO"]
            )
            installed = importlib.metadata.distribution("django-altimeter")
            direct_url = json.loads(installed.read_text("direct_url.json") or "{}")
            assert (
                direct_url["archive_info"]["hashes"]["sha256"]
                == hashlib.sha256(wheel.read_bytes()).hexdigest()
            )
            for name in names:
                if _is_package_asset(name):
                    assert installed.locate_file(
                        str(name)
                    ).read_bytes() == archive.read(str(name))


def test_sdist_contains_only_package_and_build_assets() -> None:
    sdist = _artifact("django_altimeter-*.tar.gz")
    with tarfile.open(sdist) as archive:
        members = archive.getmembers()
        roots = {PurePosixPath(member.name).parts[0] for member in members}
        assert roots == {sdist.name.removesuffix(".tar.gz")}
        assert all(member.isfile() or member.isdir() for member in members)
        assert all(
            not FORBIDDEN_PARTS.intersection(PurePosixPath(member.name).parts)
            for member in members
        )
        files = {
            PurePosixPath(*PurePosixPath(member.name).parts[1:]): archive.extractfile(
                member
            ).read()
            for member in members
            if member.isfile()
        }
    metadata = {
        PurePosixPath("src/django_altimeter.egg-info") / name
        for name in (
            "PKG-INFO",
            "SOURCES.txt",
            "dependency_links.txt",
            "requires.txt",
            "top_level.txt",
        )
    }
    build_assets = {
        PurePosixPath(name)
        for name in ("LICENSE", "README.md", "pyproject.toml", "setup.cfg", "PKG-INFO")
    }
    assert build_assets <= files.keys()
    assert {PurePosixPath("src") / path for path in REQUIRED_ASSETS} <= files.keys()
    for path in files:
        assert not path.is_absolute() and ".." not in path.parts
        assert not FORBIDDEN_PARTS.intersection(path.parts)
        assert (
            path in build_assets
            or path in metadata
            or (
                path.parts[0] == "src"
                and _is_package_asset(PurePosixPath(*path.parts[1:]))
            )
        ), f"Unexpected sdist member: {path}"
    with ZipFile(_artifact("django_altimeter-*.whl")) as wheel:
        package_files = {
            PurePosixPath(name): wheel.read(name)
            for name in wheel.namelist()
            if _is_package_asset(PurePosixPath(name))
        }
    assert {
        PurePosixPath(*path.parts[1:]): value
        for path, value in files.items()
        if path.is_relative_to("src/django_altimeter")
    } == package_files


@pytest.fixture(scope="module")
def installed_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    workspace = tmp_path_factory.mktemp("wheel-install")
    environment = workspace / "venv"
    _run([sys.executable, "-m", "venv", str(environment)], cwd=workspace)
    python = environment / "bin" / "python"
    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            str(_artifact("django_altimeter-*.whl")),
            f"Django=={importlib.metadata.version('Django')}",
        ],
        cwd=workspace,
    )
    _run([str(python), "-m", "pip", "check"], cwd=workspace)
    return python


def test_wheel_imports_without_optional_queue_libraries(
    installed_python: Path, tmp_path: Path
) -> None:
    wheel = _artifact("django_altimeter-*.whl")
    result = _run(
        [
            str(installed_python),
            "-I",
            "-c",
            """
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import sys

for optional in ("celery", "kombu", "rq"):
    assert importlib.util.find_spec(optional) is None, optional
import django_altimeter

package = Path(django_altimeter.__file__).resolve()
assert package.is_relative_to(Path(sys.prefix).resolve()), package
assert (package.parent / "py.typed").is_file()
for name in sys.modules:
    assert name.split(".")[0] not in {
        "celery", "kombu", "rq"
    }, name
installed = importlib.metadata.distribution("django-altimeter")
direct_url = json.loads(installed.read_text("direct_url.json"))
assert direct_url["archive_info"]["hashes"]["sha256"] == sys.argv[1]
assert importlib.metadata.version("Django") == sys.argv[2]
print(json.dumps({"version": installed.version, "origin": str(package)}))
""",
            hashlib.sha256(wheel.read_bytes()).hexdigest(),
            importlib.metadata.version("Django"),
        ],
        cwd=tmp_path,
    )
    assert json.loads(result.stdout)["version"] == wheel.name.split("-")[1]


def test_wheel_import_starts_no_threads(installed_python: Path, tmp_path: Path) -> None:
    _run(
        [
            str(installed_python),
            "-I",
            "-c",
            """
import _thread
import sys
import threading

started = []
thread_functions = tuple(
    getattr(_thread, name)
    for name in ("start_new_thread", "start_new", "start_joinable_thread")
    if hasattr(_thread, name)
)
def observe(frame, event, arg):
    if event == "c_call" and arg in thread_functions:
        started.append(arg.__name__)
    if event == "call" and frame.f_code is threading.Thread.start.__code__:
        started.append("Thread.start")

before = {thread.ident for thread in threading.enumerate()}
sys.setprofile(observe)
try:
    import django_altimeter
finally:
    sys.setprofile(None)
assert not started, started
assert {thread.ident for thread in threading.enumerate()} == before
assert django_altimeter.__file__.startswith(sys.prefix)
""",
        ],
        cwd=tmp_path,
    )
