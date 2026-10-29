"""Configuration behavior; Django state is isolated in real subprocesses."""

import json
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError, replace

import pytest

from django_altimeter.conf import Config, SignalConfig, load_config, validate_config
from django_altimeter.privacy import DEFAULT_LIMITS, DEFAULT_POLICY

CANARY = "configuration-secret-canary"


def test_only_explicit_resource_fields_survive_configuration(monkeypatch):
    for key in tuple(os.environ):
        if key.startswith(("ALTIMETER_", "OTEL_")):
            monkeypatch.delenv(key)
    monkeypatch.setenv("ALTIMETER_ENABLED", "true")
    monkeypatch.setenv("ALTIMETER_LOGS_ENABLED", "false")
    monkeypatch.setenv("ALTIMETER_METRICS_ENABLED", "false")
    monkeypatch.setenv(
        "ALTIMETER_TRACES_COLLECTOR_ENDPOINT", "http://127.0.0.1:4318/v1/traces"
    )
    monkeypatch.setenv("ALTIMETER_DEPLOYMENT_ENVIRONMENT", "test")
    monkeypatch.setenv("ALTIMETER_RELEASE", "v1")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "safe-service")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", f"authorization={CANARY}")
    result = load_config()
    assert result.ok and result.config is not None
    attributes = dict(result.config.resource_attributes)
    assert attributes["service.name"] == "safe-service"
    assert CANARY not in repr(attributes)


def enabled_env(**overrides):
    return {
        "ALTIMETER_ENABLED": "true",
        "ALTIMETER_SERVICE_NAME": "reference",
        "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
        "ALTIMETER_RELEASE": "v1",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
        **overrides,
    }


def parsed(environ=None, **kwargs):
    result = load_config(
        environ=enabled_env() if environ is None else environ, **kwargs
    )
    assert result.ok, result.codes
    assert result.config is not None
    return result.config


def test_defaults_disabled_frozen_and_side_effect_free():
    result = load_config(environ={})
    assert result.config == Config()
    assert result.codes == ("config.disabled",)
    assert result.config.resource_attributes == ()
    with pytest.raises(FrozenInstanceError):
        result.config.enabled = True
    assert not result.config.traces.enabled
    assert not result.config.logs.enabled
    assert not result.config.metrics.enabled
    assert result.config.sample_rate == 1.0
    assert result.config.shutdown_timeout == 2.0
    assert result.config.limits == DEFAULT_LIMITS


def test_disabled_ignores_unused_secrets_destinations_and_callbacks():
    result = load_config(
        environ={"ALTIMETER_ENABLED": "false", "ALTIMETER_TRACES_ENDPOINT": CANARY},
        values={"IDENTITY_RESOLVER": CANARY, "DESTINATIONS": CANARY},
    )
    assert result.ok and result.config == Config()
    assert CANARY not in repr(result)


def test_enabled_config_is_stable_and_retains_only_explicit_resource_metadata():
    first = parsed()
    second = parsed()
    assert first == second
    assert first.sample_rate == 1.0 and not first.trust_remote_parent
    assert all(getattr(first, name).enabled for name in ("traces", "logs", "metrics"))
    assert dict(first.resource_attributes) == {
        "service.name": "reference",
        "deployment.environment.name": "test",
        "service.version": "v1",
        "altimeter.role": "web",
    }
    assert frozenset(first.resource_attributes) <= first.policy.attribute_values
    assert "service.instance.id" not in dict(first.resource_attributes)


def test_resource_and_role_precedence():
    env = {
        "ALTIMETER_ENABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
        "OTEL_RESOURCE_ATTRIBUTES": (
            "service.name=resource,"
            "deployment.environment.name=resource-env,"
            "service.version=resource-release,altimeter.role=beat"
        ),
        "OTEL_SERVICE_NAME": "otel-service",
    }
    standard = parsed(env)
    assert (
        standard.service_name,
        standard.deployment_environment,
        standard.release,
        standard.role,
    ) == ("otel-service", "resource-env", "resource-release", "beat")
    env.update(
        ALTIMETER_SERVICE_NAME="alt-service",
        ALTIMETER_DEPLOYMENT_ENVIRONMENT="alt-env",
        ALTIMETER_RELEASE="alt-release",
        ALTIMETER_ROLE="rq-horse",
    )
    alt = parsed(env, role="celery-solo")
    assert (alt.service_name, alt.deployment_environment, alt.release, alt.role) == (
        "alt-service",
        "alt-env",
        "alt-release",
        "celery-solo",
    )
    assert parsed(env).role == "rq-horse"


@pytest.mark.parametrize(
    "key",
    ["ALTIMETER_SERVICE_NAME", "ALTIMETER_DEPLOYMENT_ENVIRONMENT", "ALTIMETER_RELEASE"],
)
def test_enabled_requires_nonempty_resource_fields_with_safe_neighbor(key):
    env = enabled_env()
    assert parsed(env).traces.enabled
    env.pop(key)
    result = load_config(environ=env)
    assert result.config is None and result.codes == ("config.invalid_value",)


@pytest.mark.parametrize(
    "role",
    [
        "web",
        "celery-prefork",
        "celery-solo",
        "beat",
        "rq-horse",
        "queue-observer",
        "rq-cron",
    ],
)
def test_closed_roles_remain_useful(role):
    assert parsed(role=role).role == role
    rejected = load_config(environ=enabled_env(), role=CANARY)
    assert rejected.config is None and rejected.codes == ("config.invalid_role",)
    assert CANARY not in repr(rejected)


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_signal_switches_and_standard_fallback_are_independent(signal):
    env = enabled_env(**{f"OTEL_{signal.upper()}_EXPORTER": "none"})
    assert not getattr(parsed(env), signal).enabled
    env[f"ALTIMETER_{signal.upper()}_ENABLED"] = "true"
    assert getattr(parsed(env), signal).enabled
    env[f"ALTIMETER_{signal.upper()}_ENABLED"] = "false"
    config = parsed(env)
    assert not getattr(config, signal).enabled
    assert all(
        getattr(config, other).enabled
        for other in {"traces", "logs", "metrics"} - {signal}
    )


def test_sampling_never_disables_metrics_and_parent_trust_is_explicit():
    env = enabled_env(
        OTEL_TRACES_SAMPLER="parentbased_traceidratio", OTEL_TRACES_SAMPLER_ARG="0.25"
    )
    standard = parsed(env)
    assert standard.sample_rate == 0.25 and standard.trust_remote_parent
    env.update(ALTIMETER_SAMPLE_RATE="0", ALTIMETER_TRUST_REMOTE_PARENT="false")
    overridden = parsed(env)
    assert overridden.sample_rate == 0 and not overridden.trust_remote_parent
    assert overridden.metrics.enabled


@pytest.mark.parametrize(
    "key,value",
    [
        ("ALTIMETER_ENABLED", "1"),
        ("ALTIMETER_TRACES_ENABLED", "yes"),
        ("ALTIMETER_SAMPLE_RATE", "NaN"),
        ("ALTIMETER_SAMPLE_RATE", "inf"),
        ("ALTIMETER_SAMPLE_RATE", "-0.1"),
        ("ALTIMETER_SAMPLE_RATE", "1.01"),
        ("ALTIMETER_MAX_ATTRIBUTES", "33"),
        ("ALTIMETER_MAX_STRING_BYTES", "257"),
        ("ALTIMETER_MAX_QUEUE_SIZE", "513"),
        ("ALTIMETER_MAX_BATCH_SIZE", "129"),
        ("ALTIMETER_MAX_BATCH_SIZE", "0"),
        ("ALTIMETER_MAX_EVENTS", "-1"),
        ("ALTIMETER_EXPORT_TIMEOUT", "2.1"),
        ("ALTIMETER_SHUTDOWN_TIMEOUT", "NaN"),
        ("ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS", "0"),
        ("ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS", "60001"),
        ("OTEL_TRACES_SAMPLER", "custom"),
        ("OTEL_LOGS_EXPORTER", "console"),
        ("ALTIMETER_INTEGRATIONS", "unknown"),
    ],
)
def test_bad_values_are_bounded_diagnostics_beside_valid_config(key, value):
    assert parsed().traces.enabled
    result = load_config(environ=enabled_env(**{key: value}))
    assert not result.ok and result.config is None
    assert len(result.codes) == 1 and result.codes[0].startswith("config.")
    assert len(result.codes[0]) <= 40


def test_limits_standard_fallback_alt_override_and_cross_field_conflicts():
    env = enabled_env(
        OTEL_ATTRIBUTE_COUNT_LIMIT="16",
        OTEL_BSP_MAX_QUEUE_SIZE="256",
        OTEL_BLRP_MAX_QUEUE_SIZE="256",
    )
    assert parsed(env).limits.max_attributes == 16
    assert parsed(env).limits.max_queue_size == 256
    env.update(
        ALTIMETER_MAX_ATTRIBUTES="8",
        ALTIMETER_MAX_QUEUE_SIZE="64",
        ALTIMETER_MAX_BATCH_SIZE="32",
    )
    assert parsed(env).limits.max_attributes == 8
    assert parsed(env).limits.max_queue_size == 64
    env["ALTIMETER_MAX_BATCH_SIZE"] = "65"
    assert load_config(environ=env).codes == ("config.invalid_limits",)
    conflict = enabled_env(
        OTEL_BSP_MAX_QUEUE_SIZE="256", OTEL_BLRP_MAX_QUEUE_SIZE="128"
    )
    assert load_config(environ=conflict).codes == ("config.limit_conflict",)


@pytest.mark.parametrize("region", ["us", "eu"])
def test_posthog_routes_and_ingest_auth_are_explicit_and_hidden(region):
    env = enabled_env(ALTIMETER_POSTHOG_TOKEN="phc_" + CANARY)
    for signal in ("traces", "logs", "metrics"):
        env[f"ALTIMETER_{signal.upper()}_ENDPOINT"] = (
            f"https://{region}.i.posthog.com/i/v1/{signal}"
        )
    config = parsed(env)
    for signal in ("traces", "logs", "metrics"):
        route = getattr(config, signal)
        assert route.destination == "posthog"
        assert route.endpoint == env[f"ALTIMETER_{signal.upper()}_ENDPOINT"]
        assert dict(route.headers) == {"authorization": "Bearer phc_" + CANARY}
    assert CANARY not in repr(config)
    assert CANARY not in repr(config.resource_attributes)
    assert CANARY not in repr(load_config(environ=env))
    env["ALTIMETER_POSTHOG_TOKEN"] = "phc_other-secret"
    assert parsed(env) != config


def test_collector_uses_its_own_auth_and_never_inherits_project_token():
    env = enabled_env(
        ALTIMETER_TRACES_COLLECTOR_ENDPOINT="https://collector.example/v1/traces",
        ALTIMETER_TRACES_COLLECTOR_HEADERS="authorization=Bearer%20private-secret",
        ALTIMETER_POSTHOG_TOKEN="phc_" + CANARY,
    )
    config = parsed(env)
    assert config.traces.destination == "collector"
    assert dict(config.traces.headers) == {"authorization": "Bearer private-secret"}
    assert "private-secret" not in repr(config)
    assert CANARY not in repr(config)
    env.pop("ALTIMETER_TRACES_COLLECTOR_HEADERS")
    assert load_config(environ=env).codes == ("config.missing_collector_auth",)


def test_collector_refuses_a_posthog_token_in_standard_headers():
    env = enabled_env(
        ALTIMETER_TRACES_COLLECTOR_ENDPOINT="https://collector.example/v1/traces",
        OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer%20phc_" + CANARY,
    )
    rejected = load_config(environ=env)
    assert rejected.config is None
    assert rejected.codes == ("config.destination_conflict",)
    assert CANARY not in repr(rejected)
    env["ALTIMETER_TRACES_COLLECTOR_HEADERS"] = "authorization=Bearer%20collector-token"
    env["ALTIMETER_LOGS_ENABLED"] = "false"
    env["ALTIMETER_METRICS_ENABLED"] = "false"
    assert parsed(env).traces.enabled


def test_destination_conflicts_fail_closed_instead_of_selecting_a_route():
    env = enabled_env(
        ALTIMETER_TRACES_ENDPOINT="https://us.i.posthog.com/i/v1/traces",
        ALTIMETER_TRACES_COLLECTOR_ENDPOINT="https://collector.example/v1/traces",
        ALTIMETER_POSTHOG_TOKEN="phc_" + CANARY,
    )
    assert load_config(environ=env).codes == ("config.destination_conflict",)
    env.pop("ALTIMETER_TRACES_COLLECTOR_ENDPOINT")
    assert parsed(env).traces.destination == "posthog"
    settings = {
        "DESTINATIONS": {
            "traces": {
                "collector_endpoint": "https://collector.example/v1/traces",
                "headers": {"x-api-key": CANARY},
            }
        }
    }
    assert load_config(environ=env, values=settings).codes == (
        "config.destination_conflict",
    )


def test_standard_signal_endpoint_is_exact_and_base_endpoint_appends_signal():
    env = enabled_env(
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://127.0.0.1:4318/custom-traces"
    )
    config = parsed(env)
    assert config.traces.endpoint == "http://127.0.0.1:4318/custom-traces"
    assert config.logs.endpoint == "http://127.0.0.1:4318/v1/logs"
    env["ALTIMETER_TRACES_COLLECTOR_ENDPOINT"] = "http://127.0.0.1:4319/alt-traces"
    assert parsed(env).traces.endpoint == env["ALTIMETER_TRACES_COLLECTOR_ENDPOINT"]


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://collector.example/v1/traces",
        "https://user:secret@collector.example/v1/traces",
        "https://collector.example/v1/traces?token=secret",
        "https://collector.example/v1/traces#secret",
        "https://collector.example:99999/v1/traces",
        "https://collector.example/\nsecret",
        "file:///tmp/collector",
        "//collector.example/v1/traces",
        "https://",
    ],
)
def test_unsafe_collector_urls_fail_without_exposing_them(endpoint):
    env = enabled_env(
        ALTIMETER_TRACES_COLLECTOR_ENDPOINT=endpoint,
        ALTIMETER_TRACES_COLLECTOR_HEADERS="x-api-key=" + CANARY,
    )
    result = load_config(environ=env)
    assert result.config is None
    assert endpoint not in repr(result) and CANARY not in repr(result)
    env["ALTIMETER_TRACES_COLLECTOR_ENDPOINT"] = "https://collector.example/v1/traces"
    assert parsed(env).traces.enabled


@pytest.mark.parametrize(
    "headers",
    [
        "authorization=x\r\ny",
        "content-type=text/plain",
        "cookie=secret",
        "x-key=a,X-Key=b",
        "missing-equals",
    ],
)
def test_invalid_headers_are_rejected_with_safe_auth_neighbor(headers):
    env = enabled_env(ALTIMETER_TRACES_COLLECTOR_HEADERS=headers)
    assert not load_config(environ=env).ok
    env["ALTIMETER_TRACES_COLLECTOR_HEADERS"] = "x-key=safe"
    assert parsed(env).traces.headers == (("x-key", "safe"),)


def test_static_settings_are_frozen_and_callbacks_are_not_invoked():
    def callback(subject):
        raise AssertionError("configuration must not call the resolver")

    values = {
        "SPAN_NAMES": ["checkout.confirm"],
        "LOG_BODIES": ["worker.ready"],
        "METRIC_INSTRUMENTS": [("django_altimeter", "runtime.calls")],
        "ATTRIBUTE_VALUES": [("http.route", "/items/<int:pk>")],
        "IDENTITY_RESOLVER": callback,
        "LOG_SCRUBBER": callback,
        "EXCLUDED_ROUTES": ["/health"],
        "FRONTEND_ORIGINS": ["https://frontend.example"],
        "INTERNAL_ORIGINS": ["https://internal.example"],
    }
    config = parsed(values=values)
    assert "checkout.confirm" in config.policy.span_names
    assert config.policy.log_bodies == frozenset({"worker.ready"})
    assert config.policy.metric_instruments == frozenset(
        {("django_altimeter", "runtime.calls")}
    )
    assert config.identity_resolver is callback and config.log_scrubber is callback
    assert config.excluded_routes == frozenset({"/health"})
    values["SPAN_NAMES"].append("later")
    assert "later" not in config.policy.span_names


def test_queue_observer_requires_dedicated_role_and_bounded_explicit_queues():
    values = {
        "BROKER_NAMESPACE": "isolated",
        "QUEUE_NAMES": ["default"],
        "QUEUE_TRANSPORT": {
            "password": CANARY,
            "ssl": True,
            "port": 6379,
            "timeout": 0.5,
        },
        "QUEUE_OBSERVER_ENABLED": True,
    }
    rejected = load_config(environ=enabled_env(), values=values)
    assert rejected.codes == ("config.invalid_observer",)
    config = parsed(values=values, role="queue-observer")
    assert config.queue_observer_enabled
    assert config.queue_names == frozenset({"default"})
    assert dict(config.queue_transport)["password"] == CANARY
    assert CANARY not in repr(config) and CANARY not in repr(config.resource_attributes)
    values["QUEUE_NAMES"] = []
    assert load_config(
        environ=enabled_env(), values=values, role="queue-observer"
    ).codes == ("config.invalid_observer",)


@pytest.mark.parametrize(
    "values",
    [
        {"UNKNOWN": CANARY},
        {"SPAN_NAMES": ["x"] * 129},
        {"SPAN_NAMES": ["é" * 129]},
        {"METRIC_INSTRUMENTS": [("django_altimeter", "runtime.calls", "extra")]},
        {"IDENTITY_RESOLVER": "not.imported.here"},
        {"QUEUE_TRANSPORT": {"nested": {"secret": CANARY}}},
        {"FRONTEND_ORIGINS": ["https://frontend.example/path"]},
    ],
)
def test_static_settings_reject_unknown_or_unbounded_shapes(values):
    assert parsed(values={"SPAN_NAMES": ["safe.operation"]}).traces.enabled
    result = load_config(environ=enabled_env(), values=values)
    assert result.config is None and len(result.codes) == 1
    assert CANARY not in repr(result)


def test_hostile_settings_and_oversized_inputs_are_not_traversed():
    calls = []

    class HostileList(list):
        def __iter__(self):
            calls.append("iter")
            raise RuntimeError(CANARY)

        def __len__(self):
            calls.append("len")
            raise RuntimeError(CANARY)

    assert not load_config(
        environ=enabled_env(), values={"SPAN_NAMES": HostileList()}
    ).ok
    assert not load_config(
        environ=enabled_env(), values={"SPAN_NAMES": [object()] * 129}
    ).ok
    assert calls == []
    assert parsed(values={"SPAN_NAMES": ["safe.operation"]}).traces.enabled


def test_direct_config_validation_is_pure_and_accepts_runtime_composition():
    config = Config(
        enabled=True,
        service_name="reference",
        deployment_environment="test",
        release="v1",
        traces=SignalConfig(True, "http://127.0.0.1:4318/v1/traces"),
        policy=replace(
            DEFAULT_POLICY,
            metric_instruments=frozenset({("django_altimeter", "runtime.calls")}),
        ),
    )
    assert validate_config(config).config is config
    for changes in (
        {"sample_rate": "0.5"},
        {"shutdown_timeout": float("nan")},
        {"limits": replace(DEFAULT_LIMITS, max_queue_size=1)},
        {"frontend_origins": ["https://frontend.example"]},
    ):
        result = validate_config(replace(config, **changes))
        assert not result.ok, changes
    assert validate_config(config).config is config


def run_python(script, tmp_path, *, env=None):
    clean = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ALTIMETER_", "OTEL_", "DJANGO_"))
    }
    clean["PYTHONPATH"] = str(tmp_path)
    clean["TEST_CONFIG_ENV"] = json.dumps(enabled_env() if env is None else env)
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=clean,
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def test_conf_and_django_boundary_import_no_django_sdk_or_worker(tmp_path):
    output = run_python(
        """
import json, sys, threading
before = set(threading.enumerate())
import django_altimeter.conf
import django_altimeter.django
print(json.dumps({"threads": len(set(threading.enumerate()) - before),
    "django": "django" in sys.modules, "sdk": "opentelemetry.sdk" in sys.modules,
    "celery": "celery" in sys.modules, "rq": "rq" in sys.modules}))
""",
        tmp_path,
        env={},
    )
    assert output == {
        "threads": 0,
        "django": False,
        "sdk": False,
        "celery": False,
        "rq": False,
    }


def test_django_callbacks_import_only_after_registry_ready_and_are_not_called(tmp_path):
    (tmp_path / "trusted_callbacks.py").write_text("""
from pathlib import Path
Path("imported").write_text("ready")
def identity(subject):
    raise AssertionError("resolver must not be called during configuration")
def scrubber(subject):
    raise AssertionError("scrubber must not be called during configuration")
""")
    output = run_python(
        """
import json, os
from pathlib import Path
from django.conf import settings
from django_altimeter.django import load_django_config
settings.configure(INSTALLED_APPS=[], ALTIMETER={
    "IDENTITY_RESOLVER": "trusted_callbacks.identity",
    "LOG_SCRUBBER": "trusted_callbacks.scrubber", "SPAN_NAMES": ["safe.operation"]})
env = json.loads(os.environ["TEST_CONFIG_ENV"])
before = load_django_config(environ=env)
assert not Path("imported").exists()
import django
django.setup()
after = load_django_config(environ=env, role="rq-cron")
assert after.config is not None, after.codes
print(json.dumps({"before": before.codes, "marker": Path("imported").read_text(),
    "identity": after.config.identity_resolver.__name__,
    "scrubber": after.config.log_scrubber.__name__, "role": after.config.role,
    "name": "safe.operation" in after.config.policy.span_names}))
""",
        tmp_path,
    )
    assert output == {
        "before": ["config.django_not_ready"],
        "marker": "ready",
        "identity": "identity",
        "scrubber": "scrubber",
        "role": "rq-cron",
        "name": True,
    }


def test_disabled_django_boundary_does_not_import_a_missing_callback(tmp_path):
    output = run_python(
        """
import json
from django.conf import settings
from django_altimeter.django import load_django_config
settings.configure(INSTALLED_APPS=[],
    ALTIMETER={"IDENTITY_RESOLVER": "missing.module.identity"})
before = load_django_config(environ={})
import django
django.setup()
after = load_django_config(environ={})
print(json.dumps({"before": before.codes, "after": after.codes,
    "enabled": after.config.enabled}))
""",
        tmp_path,
    )
    assert output == {
        "before": ["config.disabled"],
        "after": ["config.disabled"],
        "enabled": False,
    }


def test_callback_import_failure_is_fixed_code_and_application_still_runs(tmp_path):
    (tmp_path / "broken_callbacks.py").write_text(
        'raise RuntimeError("configuration-secret-canary")\n'
    )
    output = run_python(
        """
import json, os
from django.conf import settings
settings.configure(INSTALLED_APPS=[],
    ALTIMETER={"IDENTITY_RESOLVER": "broken_callbacks.identity"})
import django
django.setup()
from django_altimeter.django import load_django_config
result = load_django_config(environ=json.loads(os.environ["TEST_CONFIG_ENV"]))
application = lambda: "useful response"
print(json.dumps({"codes": result.codes, "config": result.config,
    "response": application(), "repr": repr(result)}))
""",
        tmp_path,
    )
    assert output["codes"] == ["config.callback_import"]
    assert output["config"] is None and output["response"] == "useful response"
    assert CANARY not in output["repr"]


def test_unconfigured_django_and_bad_settings_fail_open(tmp_path):
    output = run_python(
        """
import json, os
from django_altimeter.django import load_django_config
env = json.loads(os.environ["TEST_CONFIG_ENV"])
unconfigured = load_django_config(environ=env)
from django.conf import settings
settings.configure(INSTALLED_APPS=[], ALTIMETER=["invalid"])
import django
django.setup()
malformed = load_django_config(environ=env)
settings.ALTIMETER = {"IDENTITY_RESOLVER": "invalid-path"}
invalid_path = load_django_config(environ=env)
print(json.dumps([unconfigured.codes, malformed.codes, invalid_path.codes]))
""",
        tmp_path,
    )
    assert output == [
        ["config.django_not_ready"],
        ["config.invalid_settings"],
        ["config.invalid_callback"],
    ]


def header_result(headers, source):
    from urllib.parse import quote

    env = enabled_env(ALTIMETER_LOGS_ENABLED="false", ALTIMETER_METRICS_ENABLED="false")
    if source == "direct":
        config = parsed(env)
        return validate_config(
            replace(
                config, traces=replace(config.traces, headers=tuple(headers.items()))
            )
        )
    if source == "settings":
        return load_config(
            environ=env, values={"DESTINATIONS": {"traces": {"headers": headers}}}
        )
    env["ALTIMETER_TRACES_COLLECTOR_HEADERS"] = ",".join(
        f"{key}={quote(value, safe='') if source == 'encoded_environment' else value}"
        for key, value in headers.items()
    )
    return load_config(environ=env)


@pytest.mark.parametrize(
    "source", ["settings", "direct", "environment", "encoded_environment"]
)
@pytest.mark.parametrize(
    "value",
    [
        CANARY + "\u0100",
        " " + CANARY,
        CANARY + " ",
        "\t" + CANARY,
        CANARY + "\r\n",
        CANARY + "\x00",
        CANARY + "\x7f",
        CANARY + "\x85",
        "\xa0" + CANARY,
        "",
        "   ",
    ],
    ids=[
        "non-latin1",
        "leading-space",
        "trailing-space",
        "leading-tab",
        "crlf",
        "nul",
        "del",
        "c1-control",
        "leading-nbsp",
        "empty",
        "all-spaces",
    ],
)
def test_unusable_header_values_fail_closed_with_valid_auth_neighbor(source, value):
    valid_headers = {
        "authorization": "Bearer opaque-key",
        "x-client": "r\u00e9f\u00e9rence",
    }
    accepted = header_result(valid_headers, source)
    assert accepted.ok and accepted.config is not None
    assert dict(accepted.config.traces.headers) == valid_headers
    rejected = header_result({**valid_headers, "x-api-key": value}, source)
    assert rejected.config is None
    assert not rejected.ok and rejected.codes == ("config.invalid_headers",)
    assert CANARY not in repr(rejected)
    assert CANARY not in repr(accepted.config.resource_attributes)


@pytest.mark.integration
def test_configured_auth_header_exports_a_safe_span_to_real_receiver(tmp_path):
    from support.otlp_receiver import OTLPReceiver

    with OTLPReceiver() as receiver:
        result = run_python(
            """
import json, os
from django_altimeter.conf import load_config
from django_altimeter.context import get_correlation, span
from django_altimeter.runtime import setup, shutdown
config = load_config(environ=json.loads(os.environ["TEST_CONFIG_ENV"]), values={
    "DESTINATIONS": {"traces": {"headers": {
        "authorization": "Bearer opaque-key", "x-client": "r\\u00e9f\\u00e9rence"}}}})
assert config.ok and config.config is not None, config.codes
assert setup(config.config).enabled_signals == ("traces",)
with span("GET", attributes={"http.response.status_code": 200}):
    correlation = get_correlation()
    assert correlation is not None
assert shutdown().completed
print(json.dumps({"trace_id": correlation.trace_id, "span_id": correlation.span_id}))
""",
            tmp_path,
            env=enabled_env(
                ALTIMETER_LOGS_ENABLED="false",
                ALTIMETER_METRICS_ENABLED="false",
                ALTIMETER_TRACES_COLLECTOR_ENDPOINT=receiver.url("traces"),
            ),
        )
        requests = receiver.wait_for_requests()
        assert len(requests) == 1 and requests[0].path == "/v1/traces"
        request = requests[0]
        assert request.headers["Authorization"] == "Bearer opaque-key"
        assert request.headers["X-Client"] == "r\u00e9f\u00e9rence"
        spans = [
            span
            for group in request.message.resource_spans
            for scope in group.scope_spans
            for span in scope.spans
        ]
        assert len(spans) == 1 and spans[0].name == "GET"
        assert spans[0].trace_id.hex() == result["trace_id"]
        assert spans[0].span_id.hex() == result["span_id"]
        assert {item.key: item.value.int_value for item in spans[0].attributes} == {
            "http.response.status_code": 200
        }
        assert b"opaque-key" not in request.message.SerializeToString()


@pytest.mark.parametrize("source", ["environment", "settings-string"])
@pytest.mark.parametrize(
    "value", ["\ud800", "%ED%A0%80"], ids=["surrogate", "encoded-surrogate"]
)
def test_header_decoding_errors_return_only_the_header_code(source, value):
    assert header_result({"authorization": "Bearer opaque-key"}, "settings").ok
    env = enabled_env(ALTIMETER_LOGS_ENABLED="false", ALTIMETER_METRICS_ENABLED="false")
    headers = "authorization=Bearer%20opaque-key,x-api-key=" + value
    if source == "environment":
        env["ALTIMETER_TRACES_COLLECTOR_HEADERS"] = headers
        result = load_config(environ=env)
    else:
        result = load_config(
            environ=env, values={"DESTINATIONS": {"traces": {"headers": headers}}}
        )
    assert result.config is None and result.codes == ("config.invalid_headers",)
    assert "opaque-key" not in repr(result)
