import pytest

from cli.access_mangement.access_management_config_parser import (
    parse_access_management_config,
    AccessLevel,
    DataBaseAccessConfig,
)


@pytest.fixture
def sample_db_config() -> DataBaseAccessConfig:
    access_management_config = parse_access_management_config(
        {
            "databases": {
                "sample_db": {
                    "users": {
                        "user_1": {
                            "models": {
                                "staging": {
                                    "source_system_a": {
                                        "users_database": {
                                            "clients": {"+access_level": "read"},
                                            "employees": {"+access_level": "write"},
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "roles": {
                        "role_1": {
                            "models": {
                                "staging": {
                                    "source_system_a": {
                                        "users_database": {
                                            "+access_level": "read_write"
                                        }
                                    }
                                }
                            }
                        }
                    },
                }
            }
        }
    )
    return access_management_config.databases_access_config[0]


def test_parse_access_management_config_users(sample_db_config: DataBaseAccessConfig):
    user_1 = next(
        identity
        for identity in sample_db_config.access_config_identities
        if identity.identity_name == "user_1"
    )

    expected_user_1_config_paths = [
        ("/models/staging/source_system_a/users_database/clients/", AccessLevel.READ),
        (
            "/models/staging/source_system_a/users_database/employees/",
            AccessLevel.WRITE,
        ),
    ]
    assert user_1.config_paths == expected_user_1_config_paths


def test_parse_access_management_config_roles(sample_db_config: DataBaseAccessConfig):
    role_1 = next(
        identity
        for identity in sample_db_config.access_config_identities
        if identity.identity_name == "role_1"
    )

    expected_role_1_config_paths = [
        ("/models/staging/source_system_a/users_database/", AccessLevel.READ_WRITE)
    ]
    assert role_1.config_paths == expected_role_1_config_paths


def test_empty_config():
    config = parse_access_management_config({"databases": {}})
    assert len(config.databases_access_config) == 0


def test_invalid_access_level():
    invalid_config = {
        "databases": {
            "sample_db": {
                "users": {
                    "user_1": {
                        "models": {
                            "staging": {
                                "source_system_a": {
                                    "clients": {"+access_level": "invalid_level"}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    with pytest.raises(ValueError):
        parse_access_management_config(invalid_config)
