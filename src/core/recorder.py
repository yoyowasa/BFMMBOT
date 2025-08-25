# src/core/recorder.py
# 役割：イベントをNDJSONで安全に書き出す“録画係”
# - 【関数】親ディレクトリ作成
# - 【関数】1イベント=1行で追記（orjsonで高速/安全に）
# - 【関数】定期フラッシュ＆安全クローズ
from __future__ import annotations

from pathlib import Path  # パス操作
from typing import Dict, Any, Iterable  # 型ヒント
import orjson  # 高速JSON
from loguru import logger  # ログ

class NDJSONRecorder:
    """【関数】NDJSON録画器：open→write(event)→close の最小実装"""

    def __init__(self, outfile: str | Path, flush_every: int = 100) -> None:
        self.path = Path(outfile)
        self.path.parent.mkdir(parents=True, exist_ok=True)  # 親フォルダを用意
        self._fh = self.path.open("a", encoding="utf-8")
        self._n = 0
        self._flush_every = max(1, flush_every)
        logger.info(f"recording to: {self.path}")

    def write(self, event: Dict[str, Any]) -> None:
        """【関数】1イベントを書き出し（1行=1 JSON）"""
        line = orjson.dumps(event).decode("utf-8")
        self._fh.write(line + "\n")
        self._n += 1
        if self._n % self._flush_every == 0:
            self._fh.flush()

    def close(self) -> None:
        """【関数】安全クローズ（最後にflush）"""
        try:
            self._fh.flush()
        finally:
            self._fh.close()
            logger.info(f"closed: {self.path}")
