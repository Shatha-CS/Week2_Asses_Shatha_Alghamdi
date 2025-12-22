from dataclasses import dataclass
from pathlib import Path


@dataclass
class Paths:
    root: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path


def make_paths(root: Path) -> Paths:
    raw_path = root / "data" / "raw"
    cache_path = root / "data" / "cache"
    processed_path = root / "data" / "processed"
    external_path = root / "data" / "external"

    return Paths(
        root=root,
        raw=raw_path,
        cache=cache_path,
        processed=processed_path,
        external=external_path,
    )


