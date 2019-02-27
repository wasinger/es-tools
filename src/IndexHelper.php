<?php
namespace Wa72\ESTools;

use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Iterator;
use Psr\Log\LoggerAwareTrait;

/**
 * This class contains some convenience functions for working with Elasticsearch indices
 *
 */
class IndexHelper
{
    use LoggerAwareTrait;

    /**
     * @var \Elasticsearch\Client
     */
    private $client;

    /**
     * @var array
     */
    private $options = [
        'increment_separator' => '-',
        'bulk_size' => 100,

    ];

    /**
     * @param \Elasticsearch\Client $client
     * @param array $options
     */
    public function __construct(Client $client, $options = [])
    {
        $this->client = $client;
        $this->options = array_merge($this->options, $options);
    }

    /**
     * Make sure index exists and mappings, settings, and aliases are correctly set
     *
     * This function checks whether the specified index name exists and whether mappings, settings, and aliases
     * of the existing index match the specified ones.
     *
     * If the index doesn't exist, it will be created.
     *
     * If the index exists, but mappings or settings don't match, a new index will be created,
     * and the name of the new index will be returned
     *
     * @param string $index
     * @param array $mappings
     * @param array $settings
     * @param array $aliases
     * @param array $options [
     *      bool $use_alias Whether to use aliases
     *      bool $reindex_data Whether to reindex data from an existing index to a newly created index
     *  ]
     *
     * @return string|boolean The real name of the index, or false if an error occured
     */
    public function prepareIndex($index, $mappings = [], $settings = [], $aliases = [], $options = [])
    {
        $use_alias = isset($options['use_alias']) ? $options['use_alias'] : false;
        $reindex_data = isset($options['reindex_data']) ? $options['reindex_data'] : false;

        if (isset($mappings['mappings'])) $mappings = $mappings['mappings'];
        if (isset($settings['settings'])) $settings = $settings['settings'];

        $name = $index;

        if (!$this->client->indices()->exists(['index' => $index])) {
            // Index does not exist, create it
            if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " does not exist, create it");
            if ($use_alias) {
                $name = $this->createNewIndexVersion($index, $mappings, $settings);
                if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " created as alias for " . $name);
                if (!is_array($aliases)) $aliases = [];
                array_push($aliases, $index);
                $this->setAliases($name, $aliases);
            } else {
                $this->createIndex($index, $mappings, $settings);
                if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " created.");
                $name = $index;
                if (!empty($aliases)) {
                    $this->setAliases($index, $aliases);
                }
            }
        } else {
            // Index already exists, check settings and mapping
            if ($this->diffMappings($index, $mappings) || $this->diffIndexSettings($index, $settings)) {
                if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " exists, but settings are not correct.");
                if ($use_alias && $reindex_data) {
                    $name = $this->reindexToNewIndexVersion($index, $mappings, $settings, $aliases);
                } elseif ($use_alias) {
                    $name = $this->createNewIndexVersion($index, $mappings, $settings);
                    // if we don't reindex existing data, we don't set the aliases for the newly created index
                    // use $this->switchAlias() to set the alias after indexing data to the new index
                    if ($this->logger) $this->logger->warning('Wa72\ESTools\IndexHelper::prepareIndex: new version ' . $name . ' of ' . $index . " created, but no data reindexed and no aliases set");
                } else {
                    $name = false;
                    if ($this->logger) $this->logger->error('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " exists, but settings do not match.");
                }
            } else {
                // settings are correct, get current real index if it is an alias
                $name = $this->getCurrentIndexVersionName($index);
            }
        }
        return $name;
    }

    public function exists($index)
    {
        return $this->client->indices()->exists(['index' => $index]);
    }

    /**
     * Check for differences between the mappings of an existing index and the given mappings
     *
     * @param $index
     * @param $mappings
     * @return array    The differences, empty array if settings match
     *                  Settings to be added are under the "+" key.
     *                  Settings to be removed from the index are under "-" key.
     */
    public function diffMappings($index, $mappings)
    {
        if (isset($mappings['mappings'])) $mappings = $mappings['mappings'];
        self::normalizeDotPathNotation($mappings);
        $real_mappings = $this->client->indices()->getMapping(['index' => $index]);
        $key = array_keys($real_mappings)[0];
        $real_mappings = $real_mappings[$key]['mappings'];
        return self::compare_assoc_arrays($mappings, $real_mappings);
    }

    /**
     * Normalize index settings for elasticsearch
     *
     * - if a key "settings" exists, only the value of this key is used
     * - if a key "index" exists, this key is dropped and the value of this key is merged into the $settings array
     * - dot notation ('index.mapping.single_type') is converted to array ($settings['mapping']['single_type'])
     *
     * The resulting array should have the same structure as the subkey [INDEX_NAME]['settings']['index']
     * of the structure returned by the _settings endpoint form elasticsearch
     *
     *
     * @param array $settings
     * @return array mixed
     */
    public function normalizeIndexSettings($settings)
    {
        if (isset($settings['settings'])) $settings = $settings['settings'];

        self::normalizeDotPathNotation($settings);

        // move all settings out of "index" subkey
        // ($settings['index']['analysis'] => $settings['analysis'])
        foreach ($settings as $key => $value) {
            if ($key == 'index' && is_array($value)) {
                foreach ($value as $k1 => $v1) {
                    $settings[$k1] = $v1;
                }
                unset($settings['index']);
            }
        }
        return $settings;
    }

    /**
     * normalize dot path notation
     * $array['index.mapping.single_type'] -> $array['index']['mapping']['single_type']
     *
     * @param $array
     */
    static function normalizeDotPathNotation(&$array)
    {
        foreach ($array as $key => $value) {
            if (strpos($key, '.')) {
                $segments = \explode('.', $key);
                $a = &$array;
                foreach ($segments as $segment) {
                    $a = &$a[$segment];
                }
                $a = $value;
                unset($array[$key]);
            }
        }
    }

    /**
     * Check for differences between the settings of an existing index and the given settings
     *
     * Only the following settings that require reindexing are considered:
     * - analysis
     * - mapping (e.g. index.mapping.single_type)
     *
     * @param $index
     * @param $settings
     * @return array    The differences, empty array if settings match.
     *                  Settings to be added are under the "+" key.
     *                  Settings to be removed from the index are under "-" key.
     */
    public function diffIndexSettings($index, $settings)
    {
        $settings = $this->normalizeIndexSettings($settings);
        $real_settings = $this->client->indices()->getSettings(['index' => $index]);
        $key = array_keys($real_settings)[0];
        $real_settings = $real_settings[$key]['settings']['index'];
        $settings_to_consider = ['analysis', 'mapping'];
        $r = [];
        foreach ($settings_to_consider as $key) {
            if (!isset($settings[$key])) $settings[$key] = [];
            if (!empty($real_settings[$key])) {
                $diff = self::array_diff_assoc_recursive($settings[$key], $real_settings[$key]);
                if (!empty($diff)) {
                    $r['-'][$key] = $diff;
                }
                $diff2 = self::array_diff_assoc_recursive($real_settings[$key], $settings[$key]);
                if (!empty($diff2)) {
                    $r['+'][$key] = $diff2;
                }
            } else {
                // if real settings are empty, the diff is just the specified settings
                if (!empty($settings[$key])) $r['+'][$key] = $settings[$key];
            }
        }
        return $r;
    }

    /**
     * Create a new index with the name suffixed by an incremented number
     *
     * Does NOT set any aliases
     *
     * @param string $index
     * @param array $mappings
     * @param array $settings
     * @return string The incremented index name
     */
    public function createNewIndexVersion($index, $mappings = [], $settings = [])
    {
        $newname = $this->getNextIndexVersionName($index);
        $this->createIndex($newname, $mappings, $settings);
        return $newname;
    }

    /**
     * Switch an alias name to a new index
     * (The new index must be created before)
     *
     * This function also copies all additional alias names from the old real index
     * to the new index.
     *
     *
     * @param string $alias The index alias
     * @param string $real_index The real index name to which alias should point
     * @param array $additional_aliases Additional aliases to be set on the new index (and removed from the old index)
     * @return boolean
     */
    public function switchAlias($alias, $real_index, $additional_aliases = [])
    {
        $old_real_index = null;
        $old_aliases = [];
        try {
            $data = $this->client->indices()->getAlias(['name' => $alias]);
            // the alias must point to exactly 1 index
            if (count($data) == 1) {
                $old_real_index = array_keys($data)[0];
                $old_aliases = $this->getAliases($old_real_index);
                if (($key = array_search($alias, $old_aliases)) !== false) {
                    unset($old_aliases[$key]);
                }
            } else {
                return false;
            }
        } catch (Missing404Exception $e) {
            // alias does not exist, check whether there is an index with this name
            if ($this->client->indices()->exists(['index' => $alias])) {
                // the given alias name is an existing index
                $old_real_index = $alias;
                $old_aliases = array_keys($this->client->indices()->getAlias(['index' => $alias])[$alias]['aliases']);
            }
        }

        $alias_actions = [];

        // remove alias from old index, or remove index if alias was an existing index
        if ($alias === $old_real_index) { // The alias is an existing index
            $alias_actions[] = ['remove_index' => ['index' => $alias]];
        } else if ($old_real_index) {
            $alias_actions[] = ['remove' => ['index' => $old_real_index, 'alias' => $alias]];
        }

        // set the new alias
        $alias_actions[] = ['add' => ['index' => $real_index, 'alias' => $alias]];

        // set additional aliases
        if (!empty($additional_aliases)) {
            $add_aliases = array_unique(array_merge($old_aliases, $additional_aliases));
        } else {
            $add_aliases = $old_aliases;
        }
        foreach($add_aliases as $add_alias) {
            $alias_actions[] = ['add' => ['index' => $real_index, 'alias' => $add_alias]];
            if ($old_real_index !== $alias && in_array($add_alias, $old_aliases)) {
                // remove the aliases from the old index
                $alias_actions[] = ['remove' => ['index' => $old_real_index, 'alias' => $add_alias]];
            }
        }
        $response = $this->client->indices()->updateAliases([
            'body' => [
                'actions' => $alias_actions
            ]
        ]);
        return true;
    }

    /**
     * Create a new index with the name suffixed by an incremented number,
     * re-index data from the original index to the new index,
     * and set the original index name as alias to the new index
     *
     * @param string $index
     * @param array $mappings
     * @param array $settings
     * @param array $aliases Additional aliases to be set on the new index version
     * @return string The name of the new index version
     */
    public function reindexToNewIndexVersion($index, $mappings = [], $settings = [], $aliases = [])
    {
        $new_index = $this->createNewIndexVersion($index, $mappings, $settings);
        $this->reindex($index, $new_index);
        $this->switchAlias($index, $new_index, $aliases);
        return $new_index;
    }

    /**
     * Create a new index with the name suffixed by an incremented number,
     * index fresh data to the new index
     * and set the original index name as alias to the new index
     *
     * $data may be a Generator
     *
     * @param Iterator|array $documents
     * @param string $index The name of the index
     * @param array $mappings
     * @param array $settings
     * @param array $aliases
     * @return string The name of the new index version
     */
    public function indexToNewIndexVersion($documents, $index, $mappings = [], $settings = [], $aliases = [])
    {
        $new_index = $this->createNewIndexVersion($index, $mappings, $settings);
        $this->bulkIndex($index, $documents);
        $this->switchAlias($index, $new_index, $aliases);
        return $new_index;
    }

    /**
     * @param string $index The index name
     * @param iterable $documents The documents to index; may be the result of a Generator function
     * @param string|null $type The default mapping type for the documents. If not set, all documents must
     *                          have a _type key when using an elasticsearch version that still requires a mapping type.
     * @return array The _id values of the successfully indexed documents
     */
    public function bulkIndex($index, iterable $documents, $type = null)
    {
        $bulk_queries = array_filter([
            'index' => $index,
            'type' => $type,
            'body' => []
        ]);
        $count = 0;
        $ids = [];

        foreach($documents as $document) {
            if ($this->logger) $this->logger->debug('Indexing document: ' . $document['_id']);
            $count++;
            $bulk_queries['body'][] = [
                'index' => array_filter(
                    [
                        '_type' => isset($document['_type']) ? $document['_type'] : null,
                        '_id' => isset($document['_id']) ? $document['_id'] : null,
                        '_ttl' => isset($document['_ttl']) ? $document['_ttl'] : null,
                        '_routing' => isset($document['_routing']) ? $document['_routing'] : null,
                        '_parent' => isset($document['_parent']) ? $document['_parent'] : null,
                    ]
                )
            ];
            unset($document['_type'], $document['_id'], $document['_ttl'], $document['_routing'], $document['_parent']);
            $bulk_queries['body'][] = $document;
            if ($count % $this->options['bulk_size'] == 0) {
                $response = $this->client->bulk($bulk_queries);
                $ids = array_merge($ids, $this->_evaluateBulkIndexResponse($response));
                $bulk_queries['body'] = [];
            }
        }
        $response = $this->client->bulk($bulk_queries);
        $ids = array_merge($ids, $this->_evaluateBulkIndexResponse($response));
        if ($this->logger) $this->logger->info('Documents indexed: ' . count($ids));
        return $ids;
    }

    private function _evaluateBulkIndexResponse(&$response)
    {
        if ($response['errors']) {
            // in case of errors we need to loop over items
            // and check where at least one shard was successful
            $ids = [];
            foreach ($response['items'] as $item) {
                $item = $item['index'];
                if (isset($item['_shards']['successful']) && $item['_shards']['successful'] >= 1) {
                    $ids[] = $item['_id'];
                } else if (isset($item['error'])) {
                    if ($this->logger) $this->logger->error(sprintf('Bulk index error: index %s, id %s, error type: %s, Reason: %s', $item['_index'], $item['_id'], $item['error']['type'], $item['error']['reason']));
                }
            }
        } else {
            $items = array_column($response['items'], 'index');
            $ids = array_column($items, '_id');
        }
        return $ids;
    }

    /**
     * Get the real versioned index name for which an index name is an alias
     *
     * @param string $index The basename of an index that is an alias for an index version
     * @return string|boolean The index version name (the real index to which $index points)
     */
    public function getCurrentIndexVersionName($index)
    {
        try {
            $aliases = $this->client->indices()->getAlias(['name' => $index]);
        } catch (Missing404Exception $e) {
            // no alias with this name
            if ($this->client->indices()->exists(['index' => $index])) {
                // if the given index name is a real index, return the index name
                return $index;
            } else {
                // the given index does not exist
                return false;
            }
        }
        // check if the found alias points to exactly one index
        if (count($aliases) == 1) {
            return array_keys($aliases)[0];
        }
        // the alias points to more than one index, which is not allowed for index version aliases
        return false;
    }

    /**
     * Get index name suffixed by incremented number
     *
     * @param string $index The base name of the index
     * @return string The incremented index name
     */
    public function getNextIndexVersionName($index)
    {
        $basename = $index;
        $i = 0;
        $index = $basename . $this->options['increment_separator'] . $i;
        while ($this->client->indices()->exists(['index' => $index])) {
            $i++;
            $index = $basename . $this->options['increment_separator'] . $i;
        }
        return $index;
    }

    /**
     * Set aliases for an index
     *
     * @param string $index
     * @param array $aliases
     * @return array Response from server
     */
    public function setAliases($index, array $aliases)
    {
        $r = [];
        if (!empty($aliases)) {
            $alias_actions = [];
            foreach ($aliases as $alias) {
                $alias_actions[] = ['add' => ['index' => $index, 'alias' => $alias]];
            }
            $r = $this->client->indices()->updateAliases([
                'index' => $index,
                'body' => [
                    'actions' => $alias_actions
                ]
            ]);
        }
        return $r;
    }

    public function isRealIndex($index)
    {
        return $this->client->indices()->exists(['index' => $index]) && !$this->client->indices()->existsAlias(['name' => $index]);
    }

    public function isAlias($name)
    {
        return $this->client->indices()->existsAlias(['name' => $name]);
    }

    /**
     * Get all aliases of a given index
     *
     * @param $index
     * @return array
     */
    public function getAliases($index)
    {
        $response = $this->client->indices()->getAliases(['index' => $index]);
        $indices = array_keys($response);
        if ($indices[0] !== $index) throw new \InvalidArgumentException('Argument $index must be a real index, not alias');
        return array_keys($response[$index]['aliases']);
    }

    /**
     * Re-index data from $src_index to $dest_index
     *
     * $dest_index must already exist
     *
     * @param $src_index
     * @param $dest_index
     */
    public function reindex($src_index, $dest_index)
    {
        $res = $this->client->reIndex([
            'body' => [
                'source' => [
                    'index' => $src_index
                ],
                'dest' => [
                    'index' => $dest_index
                ]
            ]
        ]);
    }

    /**
     * Clean up index.
     *
     * This function lists all documents in the index and passes them to a "voter" function.
     * If this function returns TRUE the document is kept, if it returns FALSE the document
     * will be removed from the index.
     *
     * @param string $index The index to operate on
     * @param callable $voter A function which accepts an ES document as parameter and returns TRUE or FALSE
     * @param array $scroll_options Options passed to SearchScrollHelper::scrollSearch
     */
    public function cleanup($index, callable $voter, array $scroll_options =[])
    {
        if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::cleanup(): Checking for documents to delete from index...');
        $scrollhelper = new SearchScrollHelper($this->client);
        if ($this->logger) $scrollhelper->setLogger($this->logger);
        $counter = 0;
        foreach ($scrollhelper->scrollSearch($index, $scroll_options) as $hit) {
            $id = $hit['_id'];
            $type = $hit['_type'];
            $current_index = $hit['_index'];
            if (!call_user_func_array($voter, [&$hit])) {
                $params = [
                    'index' => $current_index,
                    'type' => $type,
                    'id' => $id
                ];
                $this->client->delete($params);
                $counter++;
                if ($this->logger) $this->logger->debug(sprintf('Wa72\ESTools\IndexHelper::cleanup(): deleted %s/%s/%s', $current_index, $type, $id));
            }
        }
        if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::cleanup(): ' . $counter . ' documents deleted from index.');
    }

    /**
     * Create an index
     *
     * @param string $index The name of the index to be created
     * @param array $mappings
     * @param array $settings
     * @param array $aliases
     */
    public function createIndex($index, $mappings = [], $settings = [], $aliases = [])
    {
        $body = [];
        // normalize mappings and settings array
        if (isset($mappings['mappings'])) $mappings = $mappings['mappings'];
        if (isset($settings['settings'])) $settings = $settings['settings'];
        if (!empty($settings)) {
            $body['settings'] = $settings;
        }
        if (!empty($mappings)) {
            $body['mappings'] = $mappings;
        }
        $params = [
            'index' => $index
        ];
        if (!empty($body)) {
            $params['body'] = $body;
        }
        $result = $this->client->indices()->create($params);

        if (!empty($aliases)) {
            $alias_actions = [];
            foreach ($aliases as $alias) {
                $alias_actions[] = ['add' => ['index' => $index, 'alias' => $alias]];
            }
            $this->client->indices()->updateAliases([
                'index' => $index,
                'body' => [
                    'actions' => $alias_actions
                ]
            ]);
        }
    }

    /**
     * Check that all keys from $array1 are present in $array2 and have the same value
     *
     * additional elements from $array2 are ignored
     *
     * @param array $array1
     * @param array $array2
     * @return array Array containing elements from array1 that have no match in array2
     */
    static function array_diff_assoc_recursive($array1, $array2)
    {
        $d = [];
        foreach ($array1 as $key => $value)
        {
            if (is_array($value))
            {
                if(!isset($array2[$key]) || !is_array($array2[$key])) {
                    $d[$key] = $value;
                } else {
                    $new_diff = self::array_diff_assoc_recursive($value, $array2[$key]);
                    if (!empty($new_diff))
                    {
                        $d[$key] = $new_diff;
                    }
                }
            }
            elseif (!\array_key_exists($key, $array2) || $array2[$key] != $value)
            {
                $d[$key] = $value;
            }
        }
        return $d;
    }

    /**
     * Compute difference between two multidimensional associative arrays
     *
     * @param array $array1
     * @param array $array2
     * @return array $d Associative array:
     *                  Key "-" contains elements from $array1 that have no match in $array2,
     *                  Key "+" contains elements from $array2 that have no match in $array1,
     *                  empty array if both input arrays are equal
     */
    static function compare_assoc_arrays($array1, $array2)
    {
        if (empty($array1) && empty($array2)) {
            return [];
        } elseif (empty($array1) && !empty($array2)) {
            return ['+' => $array2];
        } elseif (!empty($array1) && empty($array2)) {
            return ['-' => $array1];
        } else {
            $d = [];
            $d1 = self::array_diff_assoc_recursive($array1, $array2);
            if (!empty($d1)) {
                $d['-'] = $d1;
            }
            $d2 = self::array_diff_assoc_recursive($array2, $array1);
            if (!empty($d2)) {
                $d['+'] = $d2;
            }
            return $d;
        }
    }
}