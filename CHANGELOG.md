# Changelog

## Version 1.0.0
### Added
- Support for access management and data masking for Databricks.

### Fixed
- Bug with Redshift using model name instead of alias.

### Changed
- **[BREAKING]** Renamed macro `apply_masking_policies_for_model` → `mask_data` (for consistency across providers).
- **[BREAKING]** Renamed macro `execute_grants_for_model` → `execute_grants`.

## Version 0.3.0
### Added
- Support for `snapshot` models

## Version 0.2.7
### Fixed
- Bug where the target and vars were not passed to the dbt `run-operation` command, causing the operation to execute on the default target

## Version 0.2.6
### Added
- Macro check_should_drop_configuration_table
### Fixed
- Issue with error `1018 DETAIL: Relation does not exist` by modifying approach of updating configuration tables
### Changed
  - Updated SQL statements for building configuration tables to not drop table and execute insert+delete operations

## Version 0.2.5
### Fixed
- Bug with raising exception when user wants to configure masking on newly created column
- Bug with attaching masking policies in incremental runs

## Version 0.2.4
### Fixed
- Attaching masking policies for non-standard dbt materialization

## Version 0.2.3
### Fixed
- Attaching masking policies for models without masking configuration

## Version 0.2.2
### Fixed
- Issue with detaching policies from other project
- Issues with masking varchar columns

## Version 0.2.1
### Changed
- Updated CLI version

## Version 0.2.0
### Added
- Support for dynamic data masking in Redshift

### Changed
- Improved CLI: added new params and command
- Small code improvements: added new constants and custom exceptions

## Version 0.1.1
### Changed
- Refactored code and small improvements
  - Improved error handling
  - Added checks if previously configured identities still exists to avoid issues during invoking revoke statements
  - Renamed entity to identity
  - Added logs
  - Added checks if any row in config table has been configured
  - Added macro get_database_identities to avoid code duplication

## Version 0.1.0
### Added
- First stable version with support of access management
  - CLI wrapper for dbt commands to configure access management 
  - DBT macros for comparing access management state
  - Basic validation of provided access_management config file
  - First unit tests
