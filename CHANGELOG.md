# Changelog

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