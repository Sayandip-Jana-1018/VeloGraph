"""Project paths.

The data root follows ``VELOGRAPH_DATA_ROOT`` (set by ``scripts/env.ps1`` on the host and by docker-compose in
containers), so the same code reads and writes the same lake everywhere.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# python/src/velograph/common/settings.py -> repository root is four levels up.
REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True)
class DataPaths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def citibike_raw(self) -> Path:
        return self.raw / "citibike"

    @property
    def gbfs_recordings(self) -> Path:
        return self.raw / "gbfs_recordings"

    @property
    def lake(self) -> Path:
        return self.root / "lake"

    @property
    def serving(self) -> Path:
        return self.root / "serving"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def run(self) -> Path:
        """Runtime state of long-lived processes: PID files, locks, stop sentinels."""
        return self.root / "run"


def data_paths() -> DataPaths:
    root = Path(os.environ.get("VELOGRAPH_DATA_ROOT") or REPO_ROOT / "data")
    return DataPaths(root.resolve())
