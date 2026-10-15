PHP Helpers for Elasticsearch
====================================

[![Tests](https://github.com/wasinger/es-tools/workflows/Tests/badge.svg?branch=master)](https://github.com/wasinger/es-tools/actions)

This repository contains some useful convenience classes for working with the official Elasticsearch PHP Client, which is a rather lowlevel tool.

The tools in this repo are primarily written for my personal use but may be helpful for others too.

Currently the project contains the following classes:

- `IndexHelper`: create index, verify mappings and settings, manage index version aliases. If mappings or analysis settings have changed for an existing index, the method `prepareIndex` can automatically create a new index with the new settings, re-index existing data, and switch aliases afterwards.
- `SearchScrollHelper`: scrolling made easy
- `Index`: Class representing an Elasticsearch index

Initially, the focus was on the `IndexHelper` class mainly for easy managing of index versioning (e.g., initially you have 
an index `my-index`, then create a new version `my-index-1` and set `my-index` as an alias for `my-index-1`).
The `SearchScrollHelper` was needed for easy re-indexing existing data from the old to the new index version.
Nowadays, the `Index` class is the convenient entry point for all actions on an elasticsearch index:
managing mapping, settings, and versions, as well as indexing, querying, retrieving, and deleting documents.

There is also [wasinger/elasticsearch-bundle](https://github.com/wasinger/elasticsearch-bundle) 
for integrating these tools in a Symfony project.

Compatibility
-------------

Supports Elasticsearch 6.8, 7.17, 8.19, and 9.x with the matching major version of
`elasticsearch/elasticsearch`. CI tests PHP 8.3, 8.4, and 8.5 against Elasticsearch
6.8.23, 7.17.29, 8.19.23, and 9.5.5.

Both legacy single-type mappings (`['_doc' => ['properties' => [...]]]`) and
modern typeless mappings (`['properties' => [...]]`) are accepted, optionally
wrapped in a `mappings` key. Types are omitted from requests to Elasticsearch 7
and newer. Multiple mapping types are not supported. Mapping differences use
the server's mapping structure (typed on 6.x, typeless on 7.x and newer).

Pass a native client to the helpers as before. Versions 6/7 use
`Elasticsearch\ClientBuilder`; versions 8/9 use
`Elastic\Elasticsearch\ClientBuilder`. Use a URL including `http://` or
`https://` when configuring modern clients. Elasticsearch 9 requires a PSR-18
HTTP client, for example `composer require guzzlehttp/guzzle`.

Library methods continue to return arrays and booleans across all client
versions; `getElasticsearchClient()` returns the original native client.
Missing documents return `null` from `get()` and are safe to delete. Other
client or server errors are propagated.

Running tests
-------------

Integration tests need a running Elasticsearch server matching the installed
PHP client major version. By default `localhost:9200` is used; another host can
be set with `ES_HOST`. For example, to test Elasticsearch 9:

```bash
composer require --no-update 'elasticsearch/elasticsearch:^9.0'
composer update

docker run -d --rm --name estools-test-es -p 9210:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  -e action.destructive_requires_name=false \
  -e 'ES_JAVA_OPTS=-Xms512m -Xmx512m' \
  docker.elastic.co/elasticsearch/elasticsearch:9.5.5
ES_HOST=http://localhost:9210 vendor/bin/phpunit
docker stop estools-test-es
```

If Elasticsearch is not available, integration tests are skipped (or fail if
`CI` is set). Mapping normalization and utility tests do not require a server.

Run the GitHub Actions workflow locally using [nektos/act](https://github.com/nektos/act):

```bash
# All 12 PHP/server combinations; limit concurrency for local memory usage.
act push -j test --concurrent-jobs 1

# A single combination:
act push -j test --matrix php-version:8.5 --matrix es-version:9.5.5
```

When working behind an HTTP proxy, pass the proxy settings to the job containers,
e.g. `act push -j test --concurrent-jobs 1 --env https_proxy=$https_proxy --env http_proxy=$http_proxy --env no_proxy=localhost,127.0.0.1`.
