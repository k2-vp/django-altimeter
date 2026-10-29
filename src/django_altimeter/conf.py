"""Bounded deployment configuration, independent of Django and the OTel SDK."""

import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields, replace
from ipaddress import ip_address
from math import isfinite
from typing import cast
from urllib.parse import unquote, urlsplit

from django_altimeter.privacy import (
    DEFAULT_LIMITS,
    DEFAULT_POLICY,
    PrivacyLimits,
    PrivacyPolicy,
    is_valid_configuration,
)

Callback = Callable[[object], object | None]
Scalar = str | bool | int | float
_ROLES = frozenset(
    {
        "web",
        "celery-prefork",
        "celery-solo",
        "beat",
        "rq-horse",
        "queue-observer",
        "rq-cron",
    }
)
_INTEGRATIONS = frozenset(
    {
        "django",
        "asgi",
        "wsgi",
        "requests",
        "httpx",
        "psycopg",
        "psycopg2",
        "redis",
        "celery",
        "rq",
    }
)
_SETTINGS = frozenset(
    {
        "DESTINATIONS",
        "SPAN_NAMES",
        "EVENT_NAMES",
        "LOG_BODIES",
        "ATTRIBUTE_VALUES",
        "METRIC_INSTRUMENTS",
        "SCOPE_NAMES",
        "EXCLUDED_ROUTES",
        "INTERNAL_ORIGINS",
        "FRONTEND_ORIGINS",
        "IDENTITY_RESOLVER",
        "LOG_SCRUBBER",
        "BROKER_NAMESPACE",
        "QUEUE_NAMES",
        "QUEUE_TRANSPORT",
        "QUEUE_OBSERVER_ENABLED",
    }
)
_REGIONS = frozenset({"us.i.posthog.com", "eu.i.posthog.com"})
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9a-z-]+")
_TRANSPORT_HEADERS = frozenset(
    {
        "content-type",
        "content-length",
        "content-encoding",
        "accept-encoding",
        "transfer-encoding",
        "host",
        "cookie",
    }
)


@dataclass(frozen=True)
class SignalConfig:
    enabled: bool = False
    endpoint: str | None = field(default=None, repr=False)
    headers: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    destination: str = "none"


@dataclass(frozen=True)
class Config:
    enabled: bool = False
    service_name: str = ""
    deployment_environment: str = ""
    release: str = ""
    role: str = "web"
    traces: SignalConfig = SignalConfig()
    logs: SignalConfig = SignalConfig()
    metrics: SignalConfig = SignalConfig()
    sample_rate: float = 1.0
    trust_remote_parent: bool = False
    integrations: frozenset[str] = frozenset()
    limits: PrivacyLimits = DEFAULT_LIMITS
    policy: PrivacyPolicy = field(default=DEFAULT_POLICY, repr=False)
    export_timeout: float = 1.0
    batch_schedule_delay_millis: int = 5000
    metric_export_interval_millis: int = 10000
    shutdown_timeout: float = 2.0
    excluded_routes: frozenset[str] = frozenset()
    internal_origins: frozenset[str] = frozenset()
    frontend_origins: frozenset[str] = frozenset()
    identity_resolver: Callback | None = field(default=None, repr=False)
    log_scrubber: Callback | None = field(default=None, repr=False)
    broker_namespace: str = ""
    queue_names: frozenset[str] = frozenset()
    queue_transport: tuple[tuple[str, Scalar], ...] = field(default=(), repr=False)
    queue_observer_enabled: bool = False

    @property
    def resource_attributes(self) -> tuple[tuple[str, str], ...]:
        if not self.enabled:
            return ()
        return (
            ("service.name", self.service_name),
            ("deployment.environment.name", self.deployment_environment),
            ("service.version", self.release),
            ("altimeter.role", self.role),
        )


@dataclass(frozen=True)
class ConfigResult:
    config: Config | None
    codes: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.config is not None


class _Invalid(ValueError):
    """Contains a fixed diagnostic code, never an input value."""


def _text(value: object, maximum: int = 256, *, empty: bool = False) -> str:
    if (
        type(value) is not str
        or len(value) > maximum
        or len(value.encode("utf-8")) > maximum
        or (not empty and not value.strip())
    ):
        raise _Invalid("config.invalid_value")
    return value


def _env(environ: Mapping[str, str], name: str) -> str | None:
    value = environ.get(name)
    return None if value is None else _text(value, 8192, empty=True)


def _choose(environ: Mapping[str, str], *names: str, default: str = "") -> str:
    for name in names:
        value = _env(environ, name)
        if value is not None:
            return value
    return default


def _boolean(value: object) -> bool:
    if type(value) is bool:
        return value
    if type(value) is str and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise _Invalid("config.invalid_boolean")


def _number(
    value: object, minimum: float, maximum: float, *, integer: bool = False
) -> float:
    if type(value) not in {str, int, float}:
        raise _Invalid("config.invalid_number")
    if type(value) is str:
        _text(value, 32)
    number = float(cast(str | int | float, value))
    if (
        not isfinite(number)
        or not minimum <= number <= maximum
        or (integer and not number.is_integer())
    ):
        raise _Invalid("config.invalid_number")
    return number


def _mapping(
    value: object, allowed: frozenset[str] | None = None, maximum: int = 32
) -> dict[str, object]:
    if type(value) is not dict or len(value) > maximum:
        raise _Invalid("config.invalid_settings")
    for key in value:
        _text(key, 128)
        if allowed is not None and key not in allowed:
            raise _Invalid("config.unknown_setting")
    return cast(dict[str, object], value)


def _sequence(
    value: object, maximum: int = 128
) -> tuple[object, ...] | list[object] | set[object] | frozenset[object]:
    if type(value) not in {tuple, list, set, frozenset}:
        raise _Invalid("config.invalid_settings")
    items = cast(
        tuple[object, ...] | list[object] | set[object] | frozenset[object], value
    )
    if len(items) > maximum:
        raise _Invalid("config.invalid_settings")
    return items


def _strings(value: object, maximum: int = 128) -> frozenset[str]:
    return frozenset(_text(item) for item in _sequence(value, maximum))


def _pairs(value: object) -> frozenset[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for item in _sequence(value):
        if type(item) not in {tuple, list}:
            raise _Invalid("config.invalid_settings")
        pair = cast(tuple[object, ...] | list[object], item)
        if len(pair) != 2:
            raise _Invalid("config.invalid_settings")
        pairs.add((_text(pair[0], 128), _text(pair[1])))
    return frozenset(pairs)


def _resource_env(environ: Mapping[str, str]) -> dict[str, str]:
    raw = _choose(environ, "OTEL_RESOURCE_ATTRIBUTES")
    if not raw:
        return {}
    _text(raw, 4096)
    parts = raw.split(",")
    if len(parts) > 32:
        raise _Invalid("config.invalid_resource")
    allowed = {
        "service.name",
        "service.version",
        "deployment.environment.name",
        "deployment.environment",
        "altimeter.role",
    }
    attributes: dict[str, str] = {}
    for part in parts:
        key, separator, value = part.partition("=")
        if not separator:
            raise _Invalid("config.invalid_resource")
        if key in allowed:
            if key in attributes:
                raise _Invalid("config.invalid_resource")
            attributes[key] = _text(unquote(value, errors="strict"))
    return attributes


def _headers(value: object) -> tuple[tuple[str, str], ...]:
    if value is None or (type(value) is str and value == ""):
        return ()
    pairs: list[tuple[str, object]] = []
    if type(value) is str:
        try:
            _text(value, 8192)
            parts = value.split(",")
            if len(parts) > 16:
                raise _Invalid("config.invalid_headers")
            for part in parts:
                key, separator, item = part.partition("=")
                if not separator:
                    raise _Invalid("config.invalid_headers")
                pairs.append(
                    (
                        unquote(key.strip(), errors="strict"),
                        unquote(item, errors="strict"),
                    )
                )
        except (_Invalid, UnicodeError):
            raise _Invalid("config.invalid_headers") from None
    else:
        pairs = list(_mapping(value, maximum=16).items())
    result: dict[str, str] = {}
    for key, header_value in pairs:
        key = _text(key, 128).lower()
        try:
            header = _text(header_value, 2048, empty=True)
            # The pinned requests/http.client path encodes str headers as Latin-1.
            # _text bounds characters and UTF-8 bytes before this second encoding.
            header.encode("latin-1")
        except (_Invalid, UnicodeEncodeError):
            raise _Invalid("config.invalid_headers") from None
        if (
            not _HEADER_NAME.fullmatch(key)
            or key in result
            or key in _TRANSPORT_HEADERS
            or not header
            or header != header.strip()
            or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in header)
        ):
            raise _Invalid("config.invalid_headers")
        result[key] = header
    return tuple(sorted(result.items()))


def _url(value: object) -> tuple[str, str, bool]:
    endpoint = _text(value, 2048)
    parsed = urlsplit(endpoint)
    if (
        any(ord(char) <= 32 or ord(char) == 127 for char in endpoint)
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise _Invalid("config.invalid_endpoint")
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        raise _Invalid("config.invalid_endpoint")
    try:
        loopback = ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = False
    if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
        raise _Invalid("config.invalid_endpoint")
    return endpoint, parsed.hostname, loopback


def _has_ingest_token(headers: dict[str, str]) -> bool:
    return any(
        value.lower().startswith(("phc_", "bearer phc_")) for value in headers.values()
    )


def _signal(
    name: str, environ: Mapping[str, str], destinations: dict[str, object]
) -> SignalConfig:
    upper = name.upper()
    switch = _env(environ, f"ALTIMETER_{upper}_ENABLED")
    if switch is None:
        exporter = _choose(environ, f"OTEL_{upper}_EXPORTER", default="otlp")
        if exporter not in {"otlp", "none"}:
            raise _Invalid("config.unsupported_exporter")
        enabled = exporter == "otlp"
    else:
        enabled = _boolean(switch)
    if not enabled:
        return SignalConfig()
    values = _mapping(
        destinations.get(name, {}),
        frozenset({"posthog_endpoint", "collector_endpoint", "headers", "token"}),
    )
    posthog: object = _env(environ, f"ALTIMETER_{upper}_ENDPOINT")
    collector: object = _env(environ, f"ALTIMETER_{upper}_COLLECTOR_ENDPOINT")
    posthog = values.get("posthog_endpoint") if posthog is None else posthog
    collector = values.get("collector_endpoint") if collector is None else collector
    if posthog is not None and collector is not None:
        raise _Invalid("config.destination_conflict")
    kind = "posthog" if posthog is not None else "collector"
    endpoint = posthog if posthog is not None else collector
    if endpoint is None:
        endpoint = _env(environ, f"OTEL_EXPORTER_OTLP_{upper}_ENDPOINT")
        if endpoint is None:
            base = _env(environ, "OTEL_EXPORTER_OTLP_ENDPOINT")
            endpoint = None if base is None else base.rstrip("/") + f"/v1/{name}"
        if endpoint is None:
            raise _Invalid("config.missing_endpoint")
        kind = (
            "posthog"
            if urlsplit(_text(endpoint, 2048)).hostname in _REGIONS
            else "collector"
        )
    url, host, loopback = _url(endpoint)
    if kind == "posthog" and (
        host not in _REGIONS or urlsplit(url).path != f"/i/v1/{name}"
    ):
        raise _Invalid("config.invalid_endpoint")
    if kind == "collector" and host in _REGIONS:
        raise _Invalid("config.destination_conflict")
    header_names = [
        f"ALTIMETER_{upper}_HEADERS",
        f"OTEL_EXPORTER_OTLP_{upper}_HEADERS",
        "OTEL_EXPORTER_OTLP_HEADERS",
    ]
    if kind == "collector":
        header_names.insert(0, f"ALTIMETER_{upper}_COLLECTOR_HEADERS")
    raw_headers: object = values.get("headers")
    for header_name in header_names:
        try:
            candidate = _env(environ, header_name)
        except (_Invalid, UnicodeError):
            raise _Invalid("config.invalid_headers") from None
        if candidate is not None:
            raw_headers = candidate
            break
    headers = dict(_headers(raw_headers))
    token: object = _env(environ, "ALTIMETER_POSTHOG_TOKEN")
    if kind == "posthog":
        token = values.get("token") if token is None else token
        if token is not None:
            ingest_token = _text(token, 2048)
            if (
                not ingest_token.startswith("phc_")
                or len(ingest_token) <= 4
                or any(char.isspace() for char in ingest_token)
            ):
                raise _Invalid("config.invalid_auth")
            authorization = f"Bearer {ingest_token}"
            if "authorization" in headers and headers["authorization"] != authorization:
                raise _Invalid("config.destination_conflict")
            headers["authorization"] = authorization
        authorization = headers.get("authorization", "")
        if (
            not authorization.startswith("Bearer phc_")
            or len(authorization) <= 11
            or any(char.isspace() for char in authorization[7:])
        ):
            raise _Invalid("config.invalid_auth")
    elif (
        _has_ingest_token(headers)
        or (
            token is not None
            and any(_text(token, 2048) in value for value in headers.values())
        )
        or "token" in values
    ):
        raise _Invalid("config.destination_conflict")
    elif not loopback and not headers:
        raise _Invalid("config.missing_collector_auth")
    return SignalConfig(True, url, tuple(sorted(headers.items())), kind)


def _sampling(environ: Mapping[str, str]) -> tuple[float, bool]:
    rate = _env(environ, "ALTIMETER_SAMPLE_RATE")
    sampler = _choose(environ, "OTEL_TRACES_SAMPLER", default="always_on")
    if rate is None:
        if sampler not in {
            "always_on",
            "always_off",
            "traceidratio",
            "parentbased_always_on",
            "parentbased_always_off",
            "parentbased_traceidratio",
        }:
            raise _Invalid("config.unsupported_sampler")
        rate = (
            _choose(environ, "OTEL_TRACES_SAMPLER_ARG", default="1")
            if sampler.endswith("traceidratio")
            else ("0" if sampler.endswith("always_off") else "1")
        )
    parent = _env(environ, "ALTIMETER_TRUST_REMOTE_PARENT")
    return _number(rate, 0, 1), _boolean(
        parent
    ) if parent is not None else sampler.startswith("parentbased_")


def _limits(environ: Mapping[str, str]) -> PrivacyLimits:
    standards = {
        "max_attributes": ("OTEL_ATTRIBUTE_COUNT_LIMIT",),
        "max_string_bytes": ("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT",),
        "max_events": ("OTEL_SPAN_EVENT_COUNT_LIMIT",),
        "max_links": ("OTEL_SPAN_LINK_COUNT_LIMIT",),
        "max_queue_size": ("OTEL_BSP_MAX_QUEUE_SIZE", "OTEL_BLRP_MAX_QUEUE_SIZE"),
        "max_batch_size": (
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
            "OTEL_BLRP_MAX_EXPORT_BATCH_SIZE",
        ),
    }
    values: dict[str, int] = {}
    for item in fields(PrivacyLimits):
        ceiling = getattr(DEFAULT_LIMITS, item.name)
        raw = _env(environ, f"ALTIMETER_{item.name.upper()}")
        if raw is None:
            alternatives = {
                _env(environ, name) for name in standards.get(item.name, ())
            } - {None}
            if len(alternatives) > 1:
                raise _Invalid("config.limit_conflict")
            raw = next(iter(alternatives), str(ceiling))
        values[item.name] = int(_number(raw, 0, ceiling, integer=True))
    limits = PrivacyLimits(**values)
    if not is_valid_configuration(DEFAULT_POLICY, limits):
        raise _Invalid("config.invalid_limits")
    return limits


def _origins(value: object) -> frozenset[str]:
    origins = _strings(value)
    for origin in origins:
        endpoint, _, _ = _url(origin)
        if urlsplit(endpoint).path not in {"", "/"}:
            raise _Invalid("config.invalid_origin")
    return origins


def _callback(value: object) -> Callback | None:
    if value is None:
        return None
    if not callable(value):
        raise _Invalid("config.invalid_callback")
    return cast(Callback, value)


def _queue_transport(value: object) -> tuple[tuple[str, Scalar], ...]:
    transport = _mapping(value, maximum=16)
    for item in transport.values():
        if type(item) is str:
            _text(item, 256, empty=True)
        elif type(item) is bool:
            pass
        elif type(item) is int and -(2**63) <= item < 2**63:
            pass
        elif type(item) is float and isfinite(item):
            pass
        else:
            raise _Invalid("config.invalid_queue_transport")
    return tuple(sorted(cast(dict[str, Scalar], transport).items()))


def load_config(
    *,
    environ: Mapping[str, str] | None = None,
    values: dict[str, object] | None = None,
    role: str | None = None,
) -> ConfigResult:
    """Parse supported environment values; return only fixed codes on failure."""
    try:
        env = os.environ if environ is None else environ
        enabled = _boolean(_choose(env, "ALTIMETER_ENABLED", default="false"))
        resources = _resource_env(env) if enabled else {}
        effective_role = (
            role
            if role is not None
            else _choose(
                env, "ALTIMETER_ROLE", default=resources.get("altimeter.role", "web")
            )
        )
        if _text(effective_role, 32) not in _ROLES:
            raise _Invalid("config.invalid_role")
        if not enabled:
            return ConfigResult(Config(role=effective_role), ("config.disabled",))
        settings = _mapping({} if values is None else values, _SETTINGS)
        destinations = _mapping(
            settings.get("DESTINATIONS", {}),
            frozenset({"traces", "logs", "metrics"}),
            3,
        )
        limits = _limits(env)
        sample_rate, trust_parent = _sampling(env)
        raw_integrations = _choose(env, "ALTIMETER_INTEGRATIONS")
        integrations = _strings(raw_integrations.split(",") if raw_integrations else ())
        if not integrations <= _INTEGRATIONS:
            raise _Invalid("config.unsupported_integration")
        policy = PrivacyPolicy(
            span_names=DEFAULT_POLICY.span_names
            | _strings(settings.get("SPAN_NAMES", ())),
            event_names=DEFAULT_POLICY.event_names
            | _strings(settings.get("EVENT_NAMES", ())),
            log_bodies=_strings(settings.get("LOG_BODIES", ())),
            attribute_values=_pairs(settings.get("ATTRIBUTE_VALUES", ())),
            metric_instruments=_pairs(settings.get("METRIC_INSTRUMENTS", ())),
            scope_names=DEFAULT_POLICY.scope_names
            | _strings(settings.get("SCOPE_NAMES", ())),
        )
        observer = _env(env, "ALTIMETER_QUEUE_OBSERVER_ENABLED")
        config = Config(
            enabled=True,
            role=effective_role,
            service_name=_text(
                _choose(
                    env,
                    "ALTIMETER_SERVICE_NAME",
                    "OTEL_SERVICE_NAME",
                    default=resources.get("service.name", ""),
                )
            ),
            deployment_environment=_text(
                _choose(
                    env,
                    "ALTIMETER_DEPLOYMENT_ENVIRONMENT",
                    default=resources.get(
                        "deployment.environment.name",
                        resources.get("deployment.environment", ""),
                    ),
                )
            ),
            release=_text(
                _choose(
                    env,
                    "ALTIMETER_RELEASE",
                    default=resources.get("service.version", ""),
                )
            ),
            traces=_signal("traces", env, destinations),
            logs=_signal("logs", env, destinations),
            metrics=_signal("metrics", env, destinations),
            sample_rate=sample_rate,
            trust_remote_parent=trust_parent,
            integrations=integrations,
            limits=limits,
            policy=policy,
            export_timeout=_number(
                _choose(
                    env,
                    "ALTIMETER_EXPORT_TIMEOUT",
                    "OTEL_EXPORTER_OTLP_TIMEOUT",
                    default="1",
                ),
                0.001,
                2,
            ),
            batch_schedule_delay_millis=int(
                _number(
                    _choose(
                        env,
                        "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS",
                        "OTEL_BSP_SCHEDULE_DELAY",
                        default="5000",
                    ),
                    1,
                    60000,
                    integer=True,
                )
            ),
            metric_export_interval_millis=int(
                _number(
                    _choose(
                        env,
                        "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS",
                        "OTEL_METRIC_EXPORT_INTERVAL",
                        default="10000",
                    ),
                    1,
                    60000,
                    integer=True,
                )
            ),
            shutdown_timeout=_number(
                _choose(env, "ALTIMETER_SHUTDOWN_TIMEOUT", default="2"), 0.001, 2
            ),
            excluded_routes=_strings(settings.get("EXCLUDED_ROUTES", ())),
            internal_origins=_origins(settings.get("INTERNAL_ORIGINS", ())),
            frontend_origins=_origins(settings.get("FRONTEND_ORIGINS", ())),
            identity_resolver=_callback(settings.get("IDENTITY_RESOLVER")),
            log_scrubber=_callback(settings.get("LOG_SCRUBBER")),
            broker_namespace=_text(
                settings.get("BROKER_NAMESPACE", ""), 128, empty=True
            ),
            queue_names=_strings(settings.get("QUEUE_NAMES", ()), 32),
            queue_transport=_queue_transport(settings.get("QUEUE_TRANSPORT", {})),
            queue_observer_enabled=_boolean(
                settings.get("QUEUE_OBSERVER_ENABLED", False)
                if observer is None
                else observer
            ),
        )
        config = replace(
            config,
            policy=replace(
                policy,
                attribute_values=policy.attribute_values
                | frozenset(config.resource_attributes),
            ),
        )
        return validate_config(config)
    except _Invalid as error:
        return ConfigResult(None, (str(error),))
    except Exception:
        return ConfigResult(None, ("config.invalid",))


def validate_config(config: Config) -> ConfigResult:
    """Check direct frozen values before runtime installation, without side effects."""
    try:
        if type(config) is not Config or type(config.enabled) is not bool:
            raise _Invalid("config.invalid")
        if _text(config.role, 32) not in _ROLES:
            raise _Invalid("config.invalid_role")
        if not config.enabled:
            return ConfigResult(config, ("config.disabled",))
        if not is_valid_configuration(config.policy, config.limits):
            raise _Invalid("config.invalid_limits")
        for value in (
            config.service_name,
            config.deployment_environment,
            config.release,
        ):
            _text(value, min(256, config.limits.max_string_bytes))
        for name in ("traces", "logs", "metrics"):
            signal = getattr(config, name)
            if type(signal) is not SignalConfig or type(signal.enabled) is not bool:
                raise _Invalid("config.invalid_signal")
            if not signal.enabled:
                continue
            _, host, loopback = _url(signal.endpoint)
            if signal.destination not in {"none", "posthog", "collector"}:
                raise _Invalid("config.invalid_signal")
            if type(signal.headers) is not tuple or len(signal.headers) > 16:
                raise _Invalid("config.invalid_headers")
            headers: dict[str, object] = {}
            for pair in signal.headers:
                if (
                    type(pair) is not tuple
                    or len(pair) != 2
                    or type(pair[0]) is not str
                    or pair[0] in headers
                ):
                    raise _Invalid("config.invalid_headers")
                headers[pair[0]] = pair[1]
            checked_headers = dict(_headers(headers))
            if host in _REGIONS:
                auth = checked_headers.get("authorization", "")
                if (
                    signal.destination == "collector"
                    or urlsplit(signal.endpoint).path != f"/i/v1/{name}"
                ):
                    raise _Invalid("config.destination_conflict")
                if (
                    not auth.startswith("Bearer phc_")
                    or len(auth) <= 11
                    or any(char.isspace() for char in auth[7:])
                ):
                    raise _Invalid("config.invalid_auth")
            elif signal.destination == "posthog":
                raise _Invalid("config.destination_conflict")
            elif _has_ingest_token(checked_headers):
                raise _Invalid("config.destination_conflict")
            elif not loopback and not checked_headers:
                raise _Invalid("config.missing_collector_auth")
        for number in (
            config.sample_rate,
            config.export_timeout,
            config.shutdown_timeout,
        ):
            if type(number) not in {int, float}:
                raise _Invalid("config.invalid_number")
        for interval in (
            config.batch_schedule_delay_millis,
            config.metric_export_interval_millis,
        ):
            if type(interval) is not int:
                raise _Invalid("config.invalid_number")
        _number(config.sample_rate, 0, 1)
        if (
            type(config.trust_remote_parent) is not bool
            or type(config.queue_observer_enabled) is not bool
        ):
            raise _Invalid("config.invalid_boolean")
        if (
            type(config.integrations) is not frozenset
            or not _strings(config.integrations) <= _INTEGRATIONS
        ):
            raise _Invalid("config.unsupported_integration")
        _number(config.export_timeout, 0.001, 2)
        _number(config.shutdown_timeout, 0.001, 2)
        _number(config.batch_schedule_delay_millis, 1, 60000, integer=True)
        _number(config.metric_export_interval_millis, 1, 60000, integer=True)
        for name in ("span_names", "event_names", "log_bodies", "scope_names"):
            _strings(getattr(config.policy, name))
        _pairs(config.policy.attribute_values)
        _pairs(config.policy.metric_instruments)
        for collection in (
            config.excluded_routes,
            config.internal_origins,
            config.frontend_origins,
            config.queue_names,
        ):
            if type(collection) is not frozenset:
                raise _Invalid("config.invalid_settings")
        _strings(config.excluded_routes)
        _origins(config.internal_origins)
        _origins(config.frontend_origins)
        _callback(config.identity_resolver)
        _callback(config.log_scrubber)
        _text(config.broker_namespace, 128, empty=True)
        _strings(config.queue_names, 32)
        if (
            type(config.queue_transport) is not tuple
            or len(config.queue_transport) > 16
        ):
            raise _Invalid("config.invalid_queue_transport")
        transport: dict[str, object] = {}
        for transport_pair in config.queue_transport:
            if (
                type(transport_pair) is not tuple
                or len(transport_pair) != 2
                or type(transport_pair[0]) is not str
                or transport_pair[0] in transport
            ):
                raise _Invalid("config.invalid_queue_transport")
            transport[transport_pair[0]] = transport_pair[1]
        _queue_transport(transport)
        if config.queue_observer_enabled and (
            config.role != "queue-observer"
            or not config.broker_namespace.strip()
            or not config.queue_names
        ):
            raise _Invalid("config.invalid_observer")
        return ConfigResult(config)
    except _Invalid as error:
        return ConfigResult(None, (str(error),))
    except Exception:
        return ConfigResult(None, ("config.invalid",))
