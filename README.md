PHP Helpers for Elasticsearch
====================================

[![Build Status](https://api.travis-ci.org/wasinger/es-tools.svg?branch=master)](http://travis-ci.org/wasinger/es-tools)

This repository will contain some useful convenience classes for working with the official Elasticsearch PHP Client, which is a rather lowlevel tool.

The tools in this repo are primarily written for my personal use but may be helpful for others too.

Currently the project contains the following classes:

- `IndexHelper`: create index, verify mappings and settings, manage index version aliases. If mappings or analysis settings have changed for an existing index, the method `prepareIndex` can automatically create a new index with the new settings, re-index existing data, and switch aliases afterwards.

- `SearchScrollHelper`: scrolling made easy

See the doc comments in the code.