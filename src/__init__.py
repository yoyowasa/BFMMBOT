"""src: プロジェクトのトップレベル・パッケージ
- 役割: Pythonに「ここはパッケージです」と知らせ、`python -m src.cli...` を可能にする
- 注意: 実行処理は書かず、入口の“名札”だけを置く
"""
__version__ = "0.1.0"  # Poetryのversionと合わせる（文書の最小例に準拠）

try:
    import src._date_stamped_logs  # noqa: F401
except Exception:
    pass
