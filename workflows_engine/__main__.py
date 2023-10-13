import sys
import traceback

try:
    from workflows_engine.cli import cli
except ImportError:
    traceback.print_exc()
    print("=" * 80)
    print("Do `python -m pip install workflows_engine[ui_pinned]` first")
    sys.exit(1)
else:
    cli.main()
