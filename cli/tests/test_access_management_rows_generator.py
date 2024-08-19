from cli.access_management_config_file_parser import (
    AccessConfigEntity,
    EntityType,
    AccessLevel,
    DataBaseAccessConfig,
)
from cli.access_management_rows_generator import (
    AccessManagementRow,
    generate_access_management_rows,
)
from cli.model import ManifestNode, ModelType

# TODO: Refactor tests with parametrize and fixtures


def test_generate_access_management_rows_not_matching_paths():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[("/models", AccessLevel.READ)],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="user_type",
            model_type=ModelType.SEED,
            schema_name="staging",
            materialization="table",
            path="staging/user_type.csv",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user_type",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants=set(),
            revokes=set(),
        )
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result


def test_generate_access_management_rows_simple_case_one_user():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[("/", AccessLevel.READ)],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="user",
            model_type=ModelType.MODEL,
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_1/user.sql",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT SELECT ON staging.user TO user_1;",
                "GRANT USAGE ON SCHEMA staging TO user_1;",
            },
            revokes={"REVOKE SELECT ON staging.user FROM user_1;"},
        )
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result


def test_generate_access_management_rows_simple_case_two_users():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[("/", AccessLevel.READ)],
            ),
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_2",
                config_paths=[("/", AccessLevel.READ)],
            ),
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="user",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_1/user.sql",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT SELECT ON staging.user TO user_1;",
                "GRANT USAGE ON SCHEMA staging TO user_1;",
            },
            revokes={"REVOKE SELECT ON staging.user FROM user_1;"},
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_2",
            grants={
                "GRANT SELECT ON staging.user TO user_2;",
                "GRANT USAGE ON SCHEMA staging TO user_2;",
            },
            revokes={"REVOKE SELECT ON staging.user FROM user_2;"},
        ),
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result


def test_generate_access_management_rows_simple_case_one_user_one_role():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[("/", AccessLevel.READ)],
            ),
            AccessConfigEntity(
                entity_type=EntityType.ROLE,
                entity_name="role_2",
                config_paths=[("/", AccessLevel.READ)],
            ),
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="user",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_1/user.sql",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT SELECT ON staging.user TO user_1;",
            },
            revokes={"REVOKE SELECT ON staging.user FROM user_1;"},
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.ROLE,
            entity_name="role_2",
            grants={
                "GRANT USAGE ON SCHEMA staging TO ROLE role_2;",
                "GRANT SELECT ON staging.user TO ROLE role_2;",
            },
            revokes={"REVOKE SELECT ON staging.user FROM ROLE role_2;"},
        ),
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result


def test_generate_access_management_rows_extended_case_one_user():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[
                    ("/models/", AccessLevel.READ),
                    ("/models/staging/source_system_1/", AccessLevel.WRITE),
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="user",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_1/user.sql",
        ),
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="employee",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_2/employee.sql",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT INSERT ON staging.user TO user_1;",
                "GRANT UPDATE ON staging.user TO user_1;",
            },
            revokes={
                "REVOKE UPDATE ON staging.user FROM user_1;",
                "REVOKE INSERT ON staging.user FROM user_1;",
            },
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="employee",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT SELECT ON staging.employee TO user_1;",
            },
            revokes={"REVOKE SELECT ON staging.employee FROM user_1;"},
        ),
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result


def test_generate_access_management_rows_extended_case_one_user_exact_path():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[
                    ("/models/", AccessLevel.READ),
                    ("/models/staging/source_system_1/user/", AccessLevel.WRITE),
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="user",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_1/user.sql",
        ),
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="employee",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_2/employee.sql",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT INSERT ON staging.user TO user_1;",
                "GRANT UPDATE ON staging.user TO user_1;",
            },
            revokes={
                "REVOKE UPDATE ON staging.user FROM user_1;",
                "REVOKE INSERT ON staging.user FROM user_1;",
            },
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="employee",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT SELECT ON staging.employee TO user_1;",
            },
            revokes={"REVOKE SELECT ON staging.employee FROM user_1;"},
        ),
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result


def test_generate_access_management_rows_extended_case_two_users():
    data_base_access_config = DataBaseAccessConfig(
        database_name="some_db",
        access_config_entities=[
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_1",
                config_paths=[
                    ("/models/staging/source_system_1/", AccessLevel.WRITE),
                    ("/", AccessLevel.READ),
                ],
            ),
            AccessConfigEntity(
                entity_type=EntityType.USER,
                entity_name="user_2",
                config_paths=[("/", AccessLevel.READ)],
            ),
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="user",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_1/user.sql",
        ),
        ManifestNode(
            database_name="some_db",
            model_type=ModelType.MODEL,
            model_name="employee",
            schema_name="staging",
            materialization="table",
            path="models/staging/source_system_2/employee.sql",
        ),
    ]

    expected_result = [
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT UPDATE ON staging.user TO user_1;",
                "GRANT INSERT ON staging.user TO user_1;",
            },
            revokes={
                "REVOKE UPDATE ON staging.user FROM user_1;",
                "REVOKE INSERT ON staging.user FROM user_1;",
            },
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="user",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_2",
            grants={
                "GRANT SELECT ON staging.user TO user_2;",
                "GRANT USAGE ON SCHEMA staging TO user_2;",
            },
            revokes={"REVOKE SELECT ON staging.user FROM user_2;"},
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="employee",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_1",
            grants={
                "GRANT USAGE ON SCHEMA staging TO user_1;",
                "GRANT SELECT ON staging.employee TO user_1;",
            },
            revokes={"REVOKE SELECT ON staging.employee FROM user_1;"},
        ),
        AccessManagementRow(
            project_name="my_project",
            database_name="some_db",
            schema_name="staging",
            model_name="employee",
            materialization="table",
            entity_type=EntityType.USER,
            entity_name="user_2",
            grants={
                "GRANT SELECT ON staging.employee TO user_2;",
                "GRANT USAGE ON SCHEMA staging TO user_2;",
            },
            revokes={"REVOKE SELECT ON staging.employee FROM user_2;"},
        ),
    ]
    result = generate_access_management_rows(
        data_base_access_config, manifest_nodes, "my_project", "redshift"
    )
    assert result == expected_result
