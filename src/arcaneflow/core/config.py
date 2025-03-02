import os
from pathlib import Path
from typing import Any, Dict

try:
    import yaml
except ImportError:
    raise ImportError("PyYAML is required to load a YAML configuration. "
                      "Install with: pip install pyyaml")

DEFAULT_CONFIG_FILE = "arcaneflow.yaml"

def load_config(file_path: str = DEFAULT_CONFIG_FILE) -> Dict[str, any]:
    config_path = Path(file_path)

    if not config_path.exists():
        print(f"Config file not found at {config_path}, using empty config.")
        return {}
    
    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data:
        data = {}

    return data