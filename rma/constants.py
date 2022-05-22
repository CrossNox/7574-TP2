from typing import List

DEFAULT_PORT: int = 5678
DEFAULT_HOST: str = "0.0.0.0"

ED_KWDS: List[str] = ["university", "college", "student", "teacher", "professor"]
ED_KWDS_PATTERN: str = f'({"|".join(ED_KWDS)})'
