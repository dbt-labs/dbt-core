# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

import pytest
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

pytest_plugins = [
    "dbt.tests.fixtures.project",
    "tests.functional.v2_parser_parity.plugin",
]


@pytest.fixture(scope="session")
def otel_tracer_provider():
    """Install an in-memory OpenTelemetry exporter for the test session.

    `trace.set_tracer_provider` only takes effect the first time it is called in a
    process -- later calls log "Overriding of current TracerProvider is not allowed"
    and are ignored. Installing per test therefore leaks a span processor onto the
    first provider every time, and earlier tests' exporters keep collecting later
    tests' spans. Install once here instead; `otel_spans` gives each test isolation
    by clearing the exporter rather than by re-installing a provider.
    """
    provider = TracerProvider(resource=Resource.get_empty())
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    yield exporter
    provider.shutdown()


@pytest.fixture
def otel_spans(otel_tracer_provider):
    """The spans emitted by a single test, isolated from its neighbours."""
    otel_tracer_provider.clear()
    yield otel_tracer_provider
    otel_tracer_provider.clear()
