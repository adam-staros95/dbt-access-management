from cli.data_masking.data_masking_config_parser import (
    DataMaskingConfig,
    ModelDataMaskingConfig,
    ColumnMaskingConfig,
)
from cli.data_masking.data_masking_rows_generator import (
    DataMaskingRow,
    generate_data_masking_rows,
)
from cli.model import ManifestNode, ModelType

# TODO: Refactor tests with parametrize and fixtures


def test_generate_data_masking_rows_not_matching_models():
    data_masking_config = DataMaskingConfig(
        model_masking_identities=[
            ModelDataMaskingConfig(
                model_name="dummy_model",
                column_masking_identities=[],
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
        DataMaskingRow(
            database_name="some_db",
            schema_name="staging",
            model_name="user_type",
            materialization="table",
            masking_config=[],
        )
    ]
    result = generate_data_masking_rows(data_masking_config, manifest_nodes)
    assert result == expected_result


def test_generate_data_masking_rows_one_column_without_access():
    data_masking_config = DataMaskingConfig(
        model_masking_identities=[
            ModelDataMaskingConfig(
                model_name="dummy_model",
                column_masking_identities=[
                    ColumnMaskingConfig(
                        column_name="col1", users_with_access=[], roles_with_access=[]
                    )
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="dummy_model",
            model_type=ModelType.MODEL,
            schema_name="staging",
            materialization="table",
            path="staging/dummy_model.sql",
        ),
    ]

    expected_result = [
        DataMaskingRow(
            database_name="some_db",
            schema_name="staging",
            model_name="dummy_model",
            materialization="table",
            masking_config=[
                {
                    "column_name": "col1",
                    "users_with_access": [],
                    "roles_with_access": [],
                }
            ],
        )
    ]
    result = generate_data_masking_rows(data_masking_config, manifest_nodes)
    assert result == expected_result


def test_generate_data_masking_rows_one_user_with_access():
    data_masking_config = DataMaskingConfig(
        model_masking_identities=[
            ModelDataMaskingConfig(
                model_name="dummy_model",
                column_masking_identities=[
                    ColumnMaskingConfig(
                        column_name="col1",
                        users_with_access=["user1"],
                        roles_with_access=[],
                    )
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="dummy_model",
            model_type=ModelType.MODEL,
            schema_name="staging",
            materialization="table",
            path="staging/dummy_model.sql",
        ),
    ]

    expected_result = [
        DataMaskingRow(
            database_name="some_db",
            schema_name="staging",
            model_name="dummy_model",
            materialization="table",
            masking_config=[
                {
                    "column_name": "col1",
                    "users_with_access": ["user1"],
                    "roles_with_access": [],
                }
            ],
        )
    ]
    result = generate_data_masking_rows(data_masking_config, manifest_nodes)
    assert result == expected_result


def test_generate_data_masking_rows_one_role_with_access():
    data_masking_config = DataMaskingConfig(
        model_masking_identities=[
            ModelDataMaskingConfig(
                model_name="dummy_model",
                column_masking_identities=[
                    ColumnMaskingConfig(
                        column_name="col1",
                        users_with_access=[],
                        roles_with_access=["role1"],
                    )
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="dummy_model",
            model_type=ModelType.MODEL,
            schema_name="staging",
            materialization="table",
            path="staging/dummy_model.sql",
        ),
    ]

    expected_result = [
        DataMaskingRow(
            database_name="some_db",
            schema_name="staging",
            model_name="dummy_model",
            materialization="table",
            masking_config=[
                {
                    "column_name": "col1",
                    "users_with_access": [],
                    "roles_with_access": ["role1"],
                }
            ],
        )
    ]
    result = generate_data_masking_rows(data_masking_config, manifest_nodes)
    assert result == expected_result


def test_generate_data_masking_rows_one_column_many_roles_and_users_with_access():
    data_masking_config = DataMaskingConfig(
        model_masking_identities=[
            ModelDataMaskingConfig(
                model_name="dummy_model",
                column_masking_identities=[
                    ColumnMaskingConfig(
                        column_name="col1",
                        users_with_access=["user1", "user2"],
                        roles_with_access=["role:1", "role:2"],
                    )
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="dummy_model",
            model_type=ModelType.MODEL,
            schema_name="staging",
            materialization="table",
            path="staging/dummy_model.sql",
        ),
    ]

    expected_result = [
        DataMaskingRow(
            database_name="some_db",
            schema_name="staging",
            model_name="dummy_model",
            materialization="table",
            masking_config=[
                {
                    "column_name": "col1",
                    "users_with_access": ["user1", "user2"],
                    "roles_with_access": ["role:1", "role:2"],
                }
            ],
        )
    ]
    result = generate_data_masking_rows(data_masking_config, manifest_nodes)
    assert result == expected_result


def test_generate_data_masking_rows_many_columns_with_roles_and_users_with_access():
    data_masking_config = DataMaskingConfig(
        model_masking_identities=[
            ModelDataMaskingConfig(
                model_name="dummy_model",
                column_masking_identities=[
                    ColumnMaskingConfig(
                        column_name="col1",
                        users_with_access=["user1", "user2"],
                        roles_with_access=["role:1", "role:2"],
                    ),
                    ColumnMaskingConfig(
                        column_name="col2",
                        users_with_access=["user1"],
                        roles_with_access=["role:1", "role:2"],
                    ),
                    ColumnMaskingConfig(
                        column_name="col3",
                        users_with_access=["user1", "user2"],
                        roles_with_access=["role:2"],
                    ),
                ],
            )
        ],
    )
    manifest_nodes = [
        ManifestNode(
            database_name="some_db",
            model_name="dummy_model",
            model_type=ModelType.MODEL,
            schema_name="staging",
            materialization="table",
            path="staging/dummy_model.sql",
        ),
    ]

    expected_result = [
        DataMaskingRow(
            database_name="some_db",
            schema_name="staging",
            model_name="dummy_model",
            materialization="table",
            masking_config=[
                {
                    "column_name": "col1",
                    "users_with_access": ["user1", "user2"],
                    "roles_with_access": ["role:1", "role:2"],
                },
                {
                    "column_name": "col2",
                    "users_with_access": ["user1"],
                    "roles_with_access": ["role:1", "role:2"],
                },
                {
                    "column_name": "col3",
                    "users_with_access": ["user1", "user2"],
                    "roles_with_access": ["role:2"],
                },
            ],
        )
    ]
    result = generate_data_masking_rows(data_masking_config, manifest_nodes)
    assert result == expected_result
