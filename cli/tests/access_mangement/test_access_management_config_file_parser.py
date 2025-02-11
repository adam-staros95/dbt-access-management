from cli.access_mangement.access_management_config_parser import (
    AccessLevel,
    extract_configs,
)


def test_extract_configs_simple():
    config = {
        "models": {
            "+access_level": "read",
        }
    }
    config_paths = extract_configs(config)
    assert config_paths == [("/models/", AccessLevel.READ)]
