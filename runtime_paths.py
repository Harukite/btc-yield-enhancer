from __future__ import annotations

import os
from dataclasses import dataclass


PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


@dataclass(frozen=True)
class RuntimePaths:
    data_dir: str
    env_file: str
    state_file: str
    state_backup_dir: str
    log_dir: str


def get_runtime_paths() -> RuntimePaths:
    data_dir = os.environ.get("DATA_DIR", "").strip() or PROJECT_DIR
    data_dir = os.path.abspath(data_dir)
    return RuntimePaths(
        data_dir=data_dir,
        env_file=os.path.join(data_dir, ".env"),
        state_file=os.path.join(data_dir, "state.json"),
        state_backup_dir=os.path.join(data_dir, "state_backups"),
        log_dir=os.path.join(data_dir, "logs"),
    )
