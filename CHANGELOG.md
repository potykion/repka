<!-- https://keepachangelog.com/en/1.0.0/ -->

# Changelog

## 3.1.1

### Changed
- repka now supports 3.8 and 3.9 python versions (#59 by @Paul-Ilyin)
- changed supported pydantic version to >=1,<2 (#59 by @Paul-Ilyin)

## 3.1.0

### Added
- `repka.repositories.base.AsyncBaseRepo.get_all_aiter()` which is the same as `get_all()` method, but it returns an async iterator (#55 by @Paul-Ilyin)
- `repka.repositories.base.AsyncBaseRepo.get_by_ids_aiter()` which is the same as `get_by_ids()` method, but it returns an async iterator (#55 by @Paul-Ilyin)
- `repka.repositories.base.AsyncBaseRepo.insert_many_aiter()` method which is the same as `insert_many()` method, but it returns an async iterator (#55 by @Paul-Ilyin)
- `rerepka.repositories.base.update_or_insert_many_by_field` update or insert entities depending on field value (#63 by kosyan62)
- `rerepka.repositories.base.update_or_insert_first` same as `update_or_insert_many_by_field` but for one entity (#63 by kosyan62)

## 3.0.0 - 2020-10-14

### Changed

- `repka.repositories.base.AsyncQueryExecutor.fetch_all` - now returns AsyncIterator (#54 by @Paul-Ilyin)
- `repka.repositories.base.AsyncQueryExecutor.insert_many` - now returns AsyncIterator (#54 by @Paul-Ilyin)

### Removed

- **Breaking change!** removed [databases](https://github.com/encode/databases) support (#54 by @Paul-Ilyin)

## 2.2.0 - 2020-09-08

### Added

- `repka.repositories.base.AsyncBaseRepo` - params for raw query support (#48 by @Paul-Ilyin)

## 2.1.0 - 2020-08-21

### Added

- `repka.repositories.base.AsyncBaseRepo.update_values` - perform sql-like update (#46 by @potykion)
- `repka.repositories.base`, `repka.repositories.queries` documentation (#47 by @potykion)

## 2.0.2 - 2020-08-16

### Changed

- `repka.repositories.base.AsyncBaseRepo.insert_many` raises error in case of inconsistent `ignore_default` field values (#45 by @Paul-Ilyin) 

## 2.0.1 - 2020-08-14

### Fixed

- No import error if `databases` is not installed (#41 by @Paul-Ilyin) 

## 2.0.0 - 2020-08-14

### Added

- `repka.repositories.base.AsyncBaseRepo.ignore_default` - ignore fields which value equals to default (#38 by @Paul-Ilyin)

### Changed

- `repka.repositories.base.AsyncBaseRepo.insert_many` - perform single query instead of multiple (#33 by @Paul-Ilyin)
- `databases` as optional dependency (`poetry install -E databases` to install with `databases` dependency) (#40 by @Paul-Ilyin)

### Removed

- **Breaking change!** `repka.repositories.base.AsyncBaseRepo.ignore_insert` removed (#38 by @Paul-Ilyin)

## 1.1.0 - 2020-06-16

### Added

- `repka.utils.model_to_primitive:keep_python_primitives` - if True result dict will have python-primitives (e.g. datetime, Decimal) (#32 by potykion) 

## 1.0.0 - 2020-03-01

### Added 

- repka.api.DatabasesRepository - repo that uses [databases](https://github.com/encode/databases) lib allowing to use repka on mysql/sqlite dbs
- repka.api.FakeRepo - repo that uses list of entities instead of real database (draft, features like sqlalchemy-filters are not supported)

### Changed 

- repka.utils.model_to_primitive - returns dict of python-primitives (e.g. datetime.date is not converted to string)
- Architecture changes: queries building in separate classes; query executor class - abstrasction for aiopg & databases

### Removed 

- repka.api.ConnectionVarMixin - removed, repo supports both connection in context-var-connection in single contructor

## 0.10.1 - 2020-02-11

### Fixed

- repka.api.BaseRepository.__get_generic_type - fix type extraction for mixin-inheritance (#28 by potykion)

## 0.10.0 - 2020-01-08

### Changed

- repka.api.BaseRepository.deserialize - no need to override in inheritors (#22 by potykion)
- repka.api.BaseRepository.delete - to delete all table rows you should pass (#23 by potykion)
- repka.json_.DictJsonRepo.read_or_write_default - return tuple of data and existence
- Detailed docs

## 0.9.0 - 2020-01-04

### Added 

- repka.utils.create_async_db_connection - async context manager used to create async db connections (#20 by potykion) 
- repka.api.BaseRepository.get_all_ids - list all entities ids (#19)

## 0.8.0 - 2019-12-22

### Added

- repka.api.BaseRepository.ignore_insert - Columns will be ignored on insert while serialization, these columns will be set after insert (#17 by potykion)
- repka.json_.DictJsonRepo - typings

## 0.7.2 - 2019-11-20

### Fixed

- Update pydantic dependency

## 0.7.1 - 2019-11-12

### Fixed

- repka.api.BaseRepository.insert_many inserts entities sequentially in transaction (#16 by potykion)

## 0.7.0 - 2019-10-13

### Added

- repka.api.json_.DictJsonRepo - repository for working with json data

## 0.6.0 - 2019-10-04

### Changed

- repka.api.ConnectionVarMixin - lazy connection receive

## 0.5.0 - 2019-08-26

### Added

- repka.api.BaseRepository.update_many - update many entities in one transaction (#11 by @ivan-karavan)
- repka.api.BaseRepository.first#orders - order entities before getting first row (#10 by @potykion)

## 0.4.0 - 2019-08-08

### Added

- repka.api.BaseRepository.update_partial - update only some field of model (#9 by @ivan-karavan)
- repka.api.ConnectionVarMixin - allows to set context var somewhere and create repo without connection (#7 by @potykion)

## 0.3.0 - 2019-07-27

### Changed

- repka.api.BaseRepository.(de)serializer prop > (de)serialize method (#5 by potykion)

### Added

- repka.api.BaseRepository.get_by_id - get entities by ids (#3 by potykion)
- repka.api.BaseRepository.delete_by_id(s) - delete entity(ies) by id(s) (#6 by potykion)
- repka.api.BaseRepository.exists - check entities with filter exists (#4 by potykion)


## 0.2.0 - 2019-07-18

### Added

- repka.api.BaseRepository.get_by_id - get entity by id or None
- repka.api.BaseRepository.get_or_create - get first entity by filters or create new with defaults
- repka.api.BaseRepository.serializer property for defining serialization behaviour (#2 by @Paul-Ilyin)
- repka.api.BaseRepository.deserializer property for defining deserialization behaviour (#2 by @Paul-Ilyin)

### Removed

- repka.api.BaseRepository.entity_type property. The same functionality can be provided by deserializer property

## 0.1.0 - 2019-07-02

### Added 

- repka.api.BaseRepository - repository pattern implementation (see README#Usage for more details)
- repka.api.IdModel - pydantic base model with id