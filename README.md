# Access Management Library for dbt Models

This library is designed to manage access control for dbt models in Redshift. It provides a robust solution for configuring, validating, and applying grants and revokes to users, roles, and groups based on an access management YAML file and dbt's `manifest.json`. The library is intended for use in CI/CD pipelines and as dbt macros, ensuring that database permissions are always in sync with your dbt models.

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Usage](#usage)
    - [CI/CD Pipeline Integration](#cicd-pipeline-integration)
    - [dbt Macros](#dbt-macros)
5. [Access Management Workflow](#access-management-workflow)
6. [Example](#example)
7. [Development](#development)
8. [Contributing](#contributing)
9. [License](#license)

## Overview

The Access Management Library consists of two main components:

1. **Python Code**: This is used to configure the access control table in Redshift, validate user/role/group definitions, and execute the necessary revoke and grant statements.
2. **dbt Macros**: These macros are used as post-hooks in dbt models to dynamically apply grants based on the access configuration.

This library helps ensure that your database permissions are always aligned with the models defined in dbt, providing a streamlined way to manage access control in data environments.

---

## Installation

### Python Library

To install the Python library, you can use `pip`:

```bash
pip install your-library-name
```

### dbt Macros

Ensure that your dbt project is set up correctly, and include the macros provided by this library in your project.

---

## Configuration

### Access Management YAML

The library relies on an `access_management.yaml` file that defines the access levels for different users, roles, and groups across your dbt models.

#### Sample `access_management.yaml`

```yaml
databases:
  jaffle_shop:
    users:
      user_1:
        +access_level: read
        staging:
          +access_level: write
          some_db_1:
            some_model_a:
              access_level: read_write
          some_db_2:
            +access_level: read_write
      user_3:
        +access_level: read
        staging:
          some_db_2:
            some_model_d:
              +access_level: read_write
  test:
    users:
      user_2:
        staging:
          some_db_2:
            some_model_d:
              +access_level: read
    roles:
      some_role:
        +access_level: read_write
```

### Redshift Config Table

The library requires a configuration table in Redshift, which stores the current access levels. This table should have the following structure:

```sql
CREATE TABLE access_management.config_table (
    project_name TEXT,
    database_name TEXT,
    schema_name TEXT,
    model_name TEXT,
    materialization TEXT,
    entity_type TEXT,
    entity_name TEXT,
    grants SUPER,
    revokes SUPER
);
```

---

## Usage

### CLI

The Python code can be integrated into your CI/CD pipeline to automate the management of access control in Redshift. Here’s an example of how to use the library in your CI/CD pipeline:

1. **Configure the Temp Config Table**: The pipeline should run a script that reads the `access_management.yaml` and `manifest.json`, then populates a temporary configuration table.
  
2. **Validate Users, Roles, and Groups**: Ensure that all entities defined in the YAML file exist in Redshift.

3. **Apply Grants and Revokes**: Execute the generated SQL statements to revoke outdated permissions and apply the new ones.

```python
from your_library import AccessManager

access_manager = AccessManager(
    yaml_file="access_management.yaml",
    manifest_file="manifest.json",
    redshift_conn=your_redshift_connection
)

access_manager.validate_entities()
access_manager.update_config_table()
access_manager.apply_grants_and_revokes()
```

### dbt Macros

The library provides dbt macros that should be used as post-hooks on your dbt models. These macros read the configuration for each model and apply the necessary grants.

#### Example Macro Usage in dbt

In your dbt model:

```sql
{{ config(
    post_hook="{{ your_library.apply_grants('your_model_name') }}"
) }}

SELECT * FROM your_table;
```

This macro will automatically apply the grants configured for `your_model_name` after the model is run.

## Access Management Workflow

1. **Define Access Levels**: In `access_management.yaml`, define the access levels for each user, role, and group across your databases and models.

2. **Run the Python Script**: As part of your CI/CD pipeline, run the script to validate entities, populate the temp config table, and apply grants/revokes.

3. **Execute dbt Models**: When dbt models are run, the post-hook macros will apply the appropriate grants based on the access configuration.

4. **Audit and Review**: Regularly review the grants and revokes to ensure they meet your organization’s security policies.

---


## Example

Here is a complete example of using the library in a CI/CD pipeline and in a dbt model:

1. **CI/CD Pipeline Script**:

    ```python
    from your_library import AccessManager

    access_manager = AccessManager(
        yaml_file="access_management.yaml",
        manifest_file="manifest.json",
        redshift_conn=your_redshift_connection
    )

    access_manager.validate_entities()
    access_manager.update_config_table()
    access_manager.apply_grants_and_revokes()
    ```

2. **dbt Model Example**:
ADME.