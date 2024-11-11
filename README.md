# DBT Access Management

`dbt-access-management` is tool designed to manage access control and data masking in your dbt projects. 
Currently, only working with AWS Redshift.

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Engineering backlog](#engineering backlog)

## Overview

`dbt-access-management` tool works in two stages:

1. Validating and running desired security configuration changes and building configuration tables under `access_management` schema. 
This stage is intended to use in cicd pipelines but can be also utilized by data admins to make ad-hoc security changes
2. Running dbt macros in post-hooks to keep security state in desired place after running dbt models. 
This stage reads configuration tables made in first stage to apply grants and data masking.

### Sample diagram

![dbt-access-management](images/dbt-access-management.png)

---

## Installation

Tool is intended to be easy to use, so only required prerequisite is to have Python and DBT installed. Please always
install library with the newest tag. List of all available tags is available here: `https://github.com/adam-staros95/dbt-access-management/tags`

### CLI

To install the CLI just execute following command:

```bash
pip install git+https://github.com/adam-staros95/dbt-access-management.git@<tag>
```

### DBT macros

To install DBT macros add following package to the `packages.yml` file:
```yaml
packages:
  - git: https://github.com/adam-staros95/dbt-access-management.git
    revision: <tag>
```
and execute `dbt deps` command.

---

## Configuration

To configure library add following post-hooks under your `models` and `seeds` section of your `dbt_project.yml` file:
```yaml
models:
  jaffle_shop:
     +post-hook:
      - {{ dbt_access_management.execute_grants_for_model() }}
      - {{ dbt_access_management.apply_masking_policies_for_model() }}
seeds:
  jaffle_shop:
     +post-hook:
      - {{ dbt_access_management.execute_grants_for_model() }}
      - {{ dbt_access_management.apply_masking_policies_for_model() }}
```

Next, create `access_management.yml` and `data_masking.yml` files in your project. 
By default, files should be created under same directory as your `dbt_project.yml` file.

### `access_management.yml` file

The tool relies on an `access_management.yaml` file that defines the access levels for different users, roles, and groups across your dbt models.
If you support multiple environments on multiple databases in your DBT project you can list them in one configuration file.

#### Sample file

```yaml
databases:
  jaffle_shop_dev:
    users:
      tom:
        +access_level: read
        staging:
          +access_level: write
          hr:
            employees:
              access_level: read_write
          finance:
            +access_level: read_write
    groups:
      legal:
        +access_level: read_write
  jaffle_shop_test:
    users:
      jane:
        staging:
          hr:
            payroll:
              +access_level: read
    roles:
      marketing:
        +access_level: read
  jaffle_shop_prod:
    roles:
      admins:
        +access_level: all
```

Notes:
- `jaffle_shop_dev`, `jaffle_shop_test` and `jaffle_shop_prod` are databases in which dbt models are created.
- entity names (under `users`, `roles` and `groups` sections) are case-sensitive.

Tool supports following access levels:
- `read`
- `write`
- `read_write`
- `all`

and more specific access level overwrites less specific one.

### `data_masking.yml` file

The tool uses `data_masking.yaml` to attach dynamic data masking policies to your dbt models. 

#### Sample file

```yaml
configuration:
  - employees:
      columns:
        - first_name:
            roles_with_access:
              - hr_role
              - oauth_aad:HR_TEAM
        - last_name:
            roles_with_access:
              - hr_role
              - oauth_aad:HR_TEAM
  - clients:
      columns:
        - email:
            roles_with_access:
              - marketing
            users_with_access:
               - jane
               - tom
```
Notes:
- `employees` and `clients` are dbt_models.
- entity names (under `roles_with_access` and `users_with_access` sections) are case-sensitive.
- only configured entities will be able to read data stored in configured columns 

---

## Usage

[//]: # (TODO)

## Engineering backlog
- Adding support for snapshot models
- Adding support for column level security
- Adding support for row level security 
- Reading database system tables to keep privileges configuration in desired state to avoid situation where privileges are configured outside `dbt-access-management`
- Renaming `access_management.yml` file to `privileges.yml` file and respective configuration tables names

