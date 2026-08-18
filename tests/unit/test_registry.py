"""Unit tests for processor registry."""

from distributed_processing.processors.registry import create_default_registry


def test_registry_default_types():
    reg = create_default_registry()
    types = reg.list_types()
    assert "image:blur" in types
    assert "image:grayscale" in types
    assert "image:resize" in types
    assert "data:transform" in types

    assert reg.get("image:blur") is not None
    assert reg.get("unknown:type") is None
