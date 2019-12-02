# SQL AST Service

The best option I've found so far for parsing SQL into an AST is https://www.npmjs.com/package/flora-sql-parser.

I wrapped it up as a microservice which exposes a single REST endpoint.

To run it locally, just run `docker-compose build && docker-compose up` from the `sql-ast-parser` directory.

# TODO

## cleanup SQL parsing (handle aliases, sub-selects, etc)

## functions in projections
* depending on JSON structure this wouldn’t be that hard

cleanup operators

Document that hashing on doubles can get you into trouble.

table aliases
