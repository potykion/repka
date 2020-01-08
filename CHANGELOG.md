# Changelog

## Unreleased - 2020-01-08

### Changed

- repka.api.BaseRepository.deserialize - no need to override in inheritors (#22)
- repka.api.BaseRepository.delete - to delete all table rows you should pass (#23)
- repka.json_.DictJsonRepo.read_or_write_default - return tuple of data and existence
- Detailed docs

## 0.9.0 - 2020-01-04

### Added 

- repka.utils.create_async_db_connection - async context manager used to create async db connections (#20) 
- repka.api.BaseRepository.get_all_ids - list all entities ids (#19)

## 0.8.0 - 2019-12-22

### Added

- repka.api.BaseRepository.ignore_insert - Columns will be ignored on insert while serialization, these columns will be set after insert (#17)
- repka.json_.DictJsonRepo - typings

## 0.7.2 - 2019-11-20

### Fixed

- Update pydantic dependency

## 0.7.1 - 2019-11-12

### Fixed

- repka.api.BaseRepository.insert_many inserts entities sequentially in transaction (#16)

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