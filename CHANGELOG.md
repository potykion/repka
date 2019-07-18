# Changelog

## 0.2.0 - 2019-07-18

### Added

- repka.api.BaseRepository.get_by_id - get entity by id or None
- repka.api.BaseRepository.get_or_create - get first entity by filters or create new with defaults
- repka.api.BaseRepository.serializer property for defining serialization behaviour
- repka.api.BaseRepository.deserializer property for defining deserialization behaviour

### Removed

- repka.api.BaseRepository.entity_type property. The same functionality can be provided by deserializer property

## 0.1.0 - 2019-07-02

### Added 

- repka.api.BaseRepository - repository pattern implementation (see README#Usage for more details)
- repka.api.IdModel - pydantic base model with id