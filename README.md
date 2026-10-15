PHP Helpers for Elasticsearch and OpenSearch
===========================================

[![Tests](https://github.com/wasinger/es-tools/workflows/Tests/badge.svg?branch=master)](https://github.com/wasinger/es-tools/actions)

Convenience classes for the official Elasticsearch and OpenSearch PHP clients.

The tools in this repo are primarily written for my personal use but may be helpful for others too.

Currently the project contains the following classes:

- `IndexHelper`: create index, verify mappings and settings, manage index version aliases. If mappings or analysis settings have changed for an existing index, the method `prepareIndex` can automatically create a new index with the new settings, re-index existing data, and switch aliases afterwards.
- `SearchScrollHelper`: scrolling made easy
- `Index`: Class representing an Elasticsearch or OpenSearch index

Initially, the focus was on the `IndexHelper` class mainly for easy managing of index versioning (e.g., initially you have 
an index `my-index`, then create a new version `my-index-1` and set `my-index` as an alias for `my-index-1`).
The `SearchScrollHelper` was needed for easy re-indexing existing data from the old to the new index version.
Nowadays, the `Index` class is the convenient entry point for all actions on an Elasticsearch or OpenSearch index:
managing mapping, settings, and versions, as well as indexing, querying, retrieving, and deleting documents.

There is also [wasinger/elasticsearch-bundle](https://github.com/wasinger/elasticsearch-bundle) 
for integrating these tools in a Symfony project.

Compatibility
-------------

Install the client for your search engine explicitly. Client packages are optional
runtime dependencies, so OpenSearch applications do not need the Elasticsearch
client and Elasticsearch applications do not need the OpenSearch client.
Existing Elasticsearch applications should declare `elasticsearch/elasticsearch`
in their own `composer.json` if they previously relied on this library to install it.

| Server | PHP client package | Supported client versions |
| --- | --- | --- |
| Elasticsearch 6.8 | `elasticsearch/elasticsearch` | `^6.8.4` |
| Elasticsearch 7.17 | `elasticsearch/elasticsearch` | `^7.17` |
| Elasticsearch 8.19 | `elasticsearch/elasticsearch` | `^8.19` |
| Elasticsearch 9.x | `elasticsearch/elasticsearch` | `^9.0` |
| OpenSearch 2.x and 3.x | `opensearch-project/opensearch-php` | `^2.7` |

CI tests PHP 8.3, 8.4, and 8.5 against Elasticsearch 6.8.23, 7.17.29,
8.19.23, and 9.5.5, and OpenSearch 2.19.6 and 3.9.0. Each job installs
only the client for its selected engine and runs the complete suite.

Both legacy single-type mappings (`['_doc' => ['properties' => [...]]]`) and
modern typeless mappings (`['properties' => [...]]`) are accepted, optionally
wrapped in a `mappings` key. Types are omitted from requests to OpenSearch and Elasticsearch 7
and newer. Multiple mapping types are not supported. Mapping differences use
the server's mapping structure (typed on Elasticsearch 6.x, typeless otherwise).

Pass a native client to the helpers as before. Versions 6/7 use
`Elasticsearch\ClientBuilder`; versions 8/9 use
`Elastic\Elasticsearch\ClientBuilder`. Use a URL including `http://` or
`https://` when configuring modern clients. Elasticsearch 9 requires a PSR-18
HTTP client, for example `composer require guzzlehttp/guzzle`.

Library methods continue to return arrays and booleans across all client
versions. `getClient()` returns the original native client;
`getElasticsearchClient()` remains available as an alias for either engine.
Missing documents return `null` from `get()` and are safe to delete. Other
client or server errors are propagated.

Using OpenSearch
----------------

Install the OpenSearch client and Guzzle HTTP transport:

```bash
composer require 'opensearch-project/opensearch-php:^2.7' 'guzzlehttp/guzzle:^7.0'
```

Pass the native OpenSearch client to any helper:

```php
use OpenSearch\GuzzleClientFactory;
use Wa72\ESTools\Index;

$client = (new GuzzleClientFactory())->create([
    'base_uri' => 'http://localhost:9200',
]);
$index = new Index($client, 'articles', [
    'properties' => ['title' => ['type' => 'text']],
], [], []);
$index->prepare();
$index->index(['title' => 'First article'], '1');
$document = $index->get('1');
```

`IndexHelper` and `SearchScrollHelper` accept the same client. Mapping management,
version aliases, reindexing, bulk indexing, cleanup, and scrolling use the same
library API for both engines. Configure authentication and TLS on the native
client when connecting to a secured cluster.

Running tests
-------------

Integration tests need a running server compatible with the selected client.
Set `SEARCH_ENGINE=opensearch` to select OpenSearch; the default is
`elasticsearch`. By default `localhost:9200` is used; another host can be
set with `ES_HOST`. For example, to test Elasticsearch 9:

```bash
composer remove --dev --no-update opensearch-project/opensearch-php
composer require --dev --no-update 'elasticsearch/elasticsearch:^9.0'
composer update

docker run -d --rm --name estools-test-es -p 9210:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  -e action.destructive_requires_name=false \
  -e 'ES_JAVA_OPTS=-Xms512m -Xmx512m' \
  docker.elastic.co/elasticsearch/elasticsearch:9.5.5
ES_HOST=http://localhost:9210 vendor/bin/phpunit
docker stop estools-test-es
```

To run against OpenSearch instead:

```bash
composer remove --dev --no-update elasticsearch/elasticsearch
composer require --dev --no-update 'opensearch-project/opensearch-php:^2.7'
composer update

docker run -d --rm --name estools-test-opensearch -p 9210:9200 \
  -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
  -e DISABLE_INSTALL_DEMO_CONFIG=true -e action.destructive_requires_name=false \
  -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' \
  opensearchproject/opensearch:3.9.0
SEARCH_ENGINE=opensearch ES_HOST=http://localhost:9210 vendor/bin/phpunit
docker stop estools-test-opensearch
```

If the selected server is not available, integration tests are skipped (or fail
if `CI` is set). Mapping normalization and utility tests do not require a server.

Run the GitHub Actions workflow locally using [nektos/act](https://github.com/nektos/act):

```bash
# All 18 PHP/server combinations; limit concurrency for local memory usage.
act push -j test --concurrent-jobs 1

# A single combination:
act push -j test --matrix php-version:8.5 --matrix backend:opensearch-3
act push -j test --matrix php-version:8.5 --matrix backend:elasticsearch-9
```

When working behind an HTTP proxy, pass the proxy settings to the job containers,
e.g. `act push -j test --concurrent-jobs 1 --env https_proxy=$https_proxy --env http_proxy=$http_proxy --env no_proxy=localhost,127.0.0.1,search`.
