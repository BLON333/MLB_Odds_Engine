import os
import sys
import importlib
import zoneinfo

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _reload_utils(monkeypatch, zoneinfo_patch=None):
    if zoneinfo_patch is not None:
        monkeypatch.setattr(zoneinfo, "ZoneInfo", zoneinfo_patch)
    import utils
    return importlib.reload(utils)


def test_eastern_tz_primary(monkeypatch):
    mod = _reload_utils(monkeypatch)
    assert mod.EASTERN_TZ.key == "US/Eastern"


def test_eastern_tz_fallback_to_ny(monkeypatch):
    original = zoneinfo.ZoneInfo

    def fake_zoneinfo(name):
        if name == "US/Eastern":
            raise zoneinfo.ZoneInfoNotFoundError
        return original(name)

    mod = _reload_utils(monkeypatch, fake_zoneinfo)
    assert mod.EASTERN_TZ.key == "America/New_York"


def test_eastern_tz_fallback_to_utc(monkeypatch):
    original = zoneinfo.ZoneInfo

    def always_fail(name):
        if name in {"US/Eastern", "America/New_York"}:
            raise zoneinfo.ZoneInfoNotFoundError
        return original(name)

    mod = _reload_utils(monkeypatch, always_fail)
    assert mod.EASTERN_TZ.key == "UTC"

