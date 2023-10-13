import sys
import traceback

try:
    from computations.cli import cli
except ImportError:
    traceback.print_exc()
    print("=" * 80)
    print("Do `python -m pip install computations[ui_pinned]` first")
    sys.exit(1)
else:
    cli.main()
