import asyncio
from src.core.utils import load_config
from src.runtime.engine import PaperEngine

async def main():
    cfg = load_config('configs/base.yml')
    strategies = getattr(cfg, 'strategies', None)
    if not strategies:
        strategies = ['stall_then_strike','cancel_add_gate']
    engine = PaperEngine(cfg, strategies[0], strategies=strategies, strategy_cfg=getattr(cfg,'strategy_cfg',None))
    try:
        await asyncio.wait_for(engine.run_paper(), timeout=60)
    except asyncio.TimeoutError:
        pass

asyncio.run(main())
