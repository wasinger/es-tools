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