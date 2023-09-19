<p align="center">
  <a href="https://grafbase.com">
    <img src="https://grafbase.com/images/other/grafbase-logo-circle.png" height="96">
    <h3 align="center">Grafbase Engine</h3>
  </a>
</p>

<p align="center">
  This workspace houses the Grafbase Engine, the core of the Grafbase platform and user generated APIs.
</p>

<p align="center">
  <a href="https://grafbase.com/docs/quickstart/get-started"><strong>Quickstart</strong></a> ·
  <a href="/examples"><strong>Examples</strong></a> ·
  <a href="/templates"><strong>Templates</strong></a> ·
  <a href="https://grafbase.com/docs"><strong>Docs</strong></a> ·
  <a href="https://grafbase.com/cli"><strong>CLI</strong></a> ·
  <a href="https://grafbase.com/community"><strong>Community</strong></a> ·
  <a href="https://grafbase.com/changelog"><strong>Changelog</strong></a>
</p>

<br/>

## Structure

| Crate                                                 |                                  Description                                  |
| ----------------------------------------------------- | :---------------------------------------------------------------------------: |
| [async-runtime](crates/async-runtime)                 |            A wrapper crate for various async runtime functionality            |
| [common-types](crates/common-types)                   |              Various type definitions for the Grafbase platform               |
| [dataloader](crates/dataloader)                       |               A GraphQL dataloader implementation for Grafbase                |
| [dynamodb](crates/dynamodb)                           | An implementation of the built-in Grafbase database using DynamoDB and SQLite |
| [dynamodb-utils](crates/dynamodb-utils)               |                    Various utilities for use with DynamoDB                    |
| [engine](crates/engine)                               |                   A dynamic GraphQL engine written in Rust                    |
| [gateway-adapter](crates/gateway-adapter)             |              An adapter layer between the gateway and its environment         |
| [gateway-adapter-local](crates/gateway-adapter-local) |                 Local gateway execution engine implementation                 |
| [graph-entities](crates/graph-entities)               |          Various types for use with GraphQL on the Grafbase platform          |
| [graphql-extensions](crates/graphql-extensions)       |                            Extensions for `engine`                            |
| [integration-tests](crates/integration-tests)         |                               Integration tests                               |
| [log](crates/log)                                     |                Logging facilities for various Grafbase crates                 |
| [parser-graphql](crates/parser-graphql)               |        A GraphQL schema parser for upstream APIs connected to Grafbase        |
| [parser-openapi](crates/parser-openapi)               |              An OpenAPI schema parser for the Grafbase platform               |
| [parser-postgresql](crates/parser-postgresql)         |             Grafbase schema introspection for PostgreSQL database             |
| [parser-sdl](crates/parser-sdl)                       |    A parser that transforms GraphQL SDL into the Grafbase registry format     |
| [postgresql-types](crates/postgresql-types)           |                     Shared types for PostgreSQL connector                     |
| [runtime](crates/runtime)                             |         An abstraction over the various Grafbase runtime environments         |
| [runtime-local](crates/runtime-local)                 |          An abstraction over the Grafbase local runtime environment           |
| [search-protocol](crates/search-protocol)             |          Types related to the Grafbase platform search functionality          |
| [worker-env](crates/worker-env)                       |    A utility crate to extend `worker::Env` with additional functionality.     |
