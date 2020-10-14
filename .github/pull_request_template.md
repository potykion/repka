PR merge checklist:

- [ ] add tests for new functionality if any
- [ ] run `poetry install && pre-commit run -a`
- [ ] describe changes in CHANGELOG.md with PR/issue reference like so

    ```
    ## Unreleased

    ### Changed
    
    - `repka.repositories.base.AsyncQueryExecutor.fetch_all` - now returns AsyncIterator (#54 by @Paul-Ilyin)
    - `repka.repositories.base.AsyncQueryExecutor.insert_many` - now returns AsyncIterator (#54 by @Paul-Ilyin)
    ```

- [ ] ensure [build passed](https://travis-ci.org/github/potykion/repka)