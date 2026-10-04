# conftest.py — shared pytest config
# Adds src/ to sys.path so that `from transforms.xxx import yyy` works.

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
