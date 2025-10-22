"""Runtime monkey patches to add JST date prefixes to NDJSON filenames.

- OrderLog/TradeLog/DecisionLog: if NDJSON mirror is provided, rewrite it to
  include a YYYYMMDD prefix (JST) at instantiation time.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from src.core.logs import OrderLog, TradeLog
    from src.core.analytics import DecisionLog
except Exception:
    OrderLog = TradeLog = DecisionLog = None  # type: ignore


def _date_prefix_path(p: Path) -> Path:
    jst = timezone(timedelta(hours=9))
    tag = datetime.now(timezone.utc).astimezone(jst).strftime("%Y%m%d")
    return p.parent / f"{tag}{p.name}"


def _patch_mirror_init(cls):
    if cls is None:
        return
    orig_init = cls.__init__

    def wrapped(self, *args, **kwargs):  # type: ignore
        orig_init(self, *args, **kwargs)
        try:
            p = getattr(self, "_mirror", None)
            if isinstance(p, (str, Path)):
                p = Path(p)
            if isinstance(p, Path):
                newp = _date_prefix_path(p)
                self._mirror = newp
                newp.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    cls.__init__ = wrapped  # type: ignore


for _cls in (OrderLog, TradeLog, DecisionLog):
    _patch_mirror_init(_cls)

