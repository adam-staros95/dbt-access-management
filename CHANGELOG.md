# Changelog

## Version 0.3.0
- Added support for `snapshot` models

## Version 0.2.7
- Fixed a bug where the target and vars were not passed to the dbt `run-operation` command, causing the operation to execute on the default target

## Version 0.2.6
- fixed issue with error `1018 DETAIL: Relation does not exist` by modifying approach of updating configuration tables
  - added macro check_should_drop_configuration_table
  - updated SQL statements for building configuration tables to not drop table and execute insert+delete operations

## Version 0.2.5
- fixed bug with raising exception when user wants to configure masking on newly created column
- fixed bug with attaching masking policies in incremental runs

## Version 0.2.4
- fixed attaching masking policies for non-standard dbt materialization

## Version 0.2.3
- fixed attaching masking policies for models without masking configuration

## Version 0.2.2
- fixed issue with detaching policies from other project
- fixed issues with masking varchar columns

## Version 0.2.1
- updated CLI version

## Version 0.2.0
- added support for dynamic data masking in Redshift
- improved CLI: added new params and command
- small code improvements: added new constants and custom exceptions

## Version 0.1.1
- refactored code and small improvements
  - improved error handling
  - added checks if previously configured identities still exists to avoid issues during invoking revoke statements
  - renamed entity to identity
  - added logs
  - added checks if any row in config table has been configured
  - added macro get_database_identities to avoid code duplication

## Version 0.1.0
- first stable version with support of access management
  - cli wrapper for dbt commands to configure access management 
  - dbt macros for comparing access management state
  - basic validation of provided access_management config file
  - first unit tests
