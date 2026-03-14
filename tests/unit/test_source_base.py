"""Unit tests for the abstract Source base class."""

from __future__ import annotations

from denbust.sources.base import Source


class TestSourceBase:
    """Tests for abstract Source protocol bodies."""

    def test_name_property_body_returns_ellipsis(self) -> None:
        """The abstract property body should still be executable for coverage."""
        getter = Source.name.fget

        assert getter is not None
        assert getter(object()) is None

    async def test_fetch_body_returns_ellipsis(self) -> None:
        """The abstract async body should still be executable for coverage."""
        result = await Source.fetch(object(), 1, [])

        assert result is None
