import pytest
import yaml
from pathlib import Path
from src.arcaneflow.core.config import load_config


@pytest.fixture
def temp_config_file(tmp_path):
    """Creates a temporary YAML config file for testing."""
    config_data = {"key1": "value1", "key2": 42}
    config_file = tmp_path / "test_config.yaml"

    with config_file.open("w", encoding="utf-8") as f:
        yaml.dump(config_data, f)

    return config_file  # Return the temporary file path


def test_load_valid_config(temp_config_file):
    """Test loading a valid YAML config file."""
    config = load_config(str(temp_config_file))
    assert isinstance(config, dict)
    assert config["key1"] == "value1"
    assert config["key2"] == 42


def test_load_missing_config():
    """Test behavior when the config file does not exist."""
    config = load_config("non_existent_file.yaml")
    assert config == {}  # Should return an empty dictionary


def test_load_empty_config(tmp_path):
    """Test loading an empty YAML config file."""
    empty_config_file = tmp_path / "empty_config.yaml"
    empty_config_file.touch()  # Creates an empty file

    config = load_config(str(empty_config_file))
    assert config == {}  # Should return an empty dictionary
