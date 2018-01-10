<?php
namespace Wa72\ESTools;

use Elasticsearch\Client;
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

    private $options = [
        'increment_separator' => '-'
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
     * @param array $options
     *
     * @return string|boolean The real name of the index, or false if an error occured
     */
    public function prepareIndex($index, $mappings = [], $settings = [], $options = [])
    {
        $use_alias = isset($options['use_alias']) ? $options['use_alias'] : false;

        $name = $index;

        if (!$this->client->indices()->exists(['index' => $index])) {
            if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " does not exist, create it");
            if ($use_alias) {
                $name = $this->createNewIndexVersion($index, $mappings, $settings);
            } else {
                $this->createIndex($index, $mappings, $settings);
                $name = $index;
            }
        } else {
            $analysis = isset($settings['analysis']) ? $settings['analysis'] : [];
            if ($this->diffMappings($index, $mappings) || $this->diffAnalysis($index, $analysis)) {
                if ($use_alias) {
                    $name = $this->createNewIndexVersion($index, $mappings, $settings);
                } else {
                    $name = false;
                    if ($this->logger) $this->logger->error('Wa72\ESTools\IndexHelper::prepareIndex: index ' . $index . " exists, but settings do not match");
                }
            }
        }
        return $name;
    }

    /**
     * @param $index
     * @param $mappings
     * @return array
     */
    public function diffMappings($index, $mappings)
    {
        $real_mappings = $this->client->indices()->getMapping(['index' => $index]);
        $key = array_keys($real_mappings)[0];
        $real_mappings = $real_mappings[$key]['mappings'];
        $r = self::array_diff_assoc_recursive($mappings, $real_mappings);
        return $r;
    }

    /**
     * @param $index
     * @param $analysis
     * @return array
     */
    public function diffAnalysis($index, $analysis)
    {
        $real_settings = $this->client->indices()->getSettings(['index' => $index]);
        $key = array_keys($real_settings)[0];
        $real_settings = $real_settings[$key]['settings']['index'];
        if (!empty($real_settings['analysis'])) {
            $r = self::array_diff_assoc_recursive($analysis, $real_settings['analysis']);
        } else {
            // if real analysis settings are empty, the diff is just the specified settings
            $r = $analysis;
        }
        return $r;
    }

    /**
     * Create a new index with the name suffixed by an incremented number
     *
     * @param string $index
     * @param array $mappings
     * @param array $settings
     * @return string The incremented index name
     */
    public function createNewIndexVersion($index, $mappings = [], $settings = [])
    {
        $newname = $this->getNextFreeIndexName($index);
        $this->createIndex($newname, $mappings, $settings);
        return $newname;
    }

    /**
     * Create a new index with the name suffixed by an incremented number,
     * re-index data from the original index to the new index,
     * and set the original index name as alias to the new index
     *
     * @param string $index
     * @param array $mappings
     * @param array $settings
     * @param array $aliases
     */
    public function reindexToNewIndexVersion($index, $mappings = [], $settings = [], $aliases = [])
    {
        $new_index = $this->createNewIndexVersion($index, $mappings, $settings);
        $this->reindex($index, $new_index);
        $aliases = array_merge($aliases, $index);
        $this->setAliases($new_index, $aliases);
    }

    /**
     * Get index name suffixed by incremented number
     *
     * @param string $index The base name of the index
     * @return string The incremented index name
     */
    public function getNextFreeIndexName($index)
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
     */
    public function setAliases($index, array $aliases)
    {
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
     * Prüfe, ob Index schon vorhandenen ist,
     * und erstelle Mapping für den Type
     *
     * @param string index
     * @param array $mappings
     * @param array $analysis
     * @param array $aliases
     */
    public function check_index($index, $mappings = [], $analysis = [], $aliases = [])
    {
        // check whether index exists, if not create it
        if (!$this->client->indices()->exists(['index' => $index])) {
            if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::check_index: index ' . $index . " does not exist, create it");
            $body = [];
            if (!empty($this->analysis)) {
                $body['settings'] = [
                    'analysis' => $analysis
                ];
            }
            if (!empty($mapping)) {
                $body['mappings'] = $mappings;
            }
            $params = [
                'index' => $index
            ];
            if (!empty($body)) {
                $params['body'] = $body;
            }
            $this->client->indices()->create($params);
            $alias_actions = [];
            if (!empty($aliases)) {
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
        } elseif (!empty($mappings)) {
            // Wiederholtes putMapping ist kein Problem,
            // solange es keine Konflikte zu vorherigen Mappings
            // sowie Mappings von anderen Types gibt.
            // Im Falle eine Konfliktes gibt es eine Exception.
            $this->client->indices()->putMapping([
                'index' => $index,
                'body' => $mappings
            ]);
        }
    }
    static function array_diff_assoc_recursive($array1, $array2)
    {
        $difference = [];
        foreach ($array1 as $key => $value)
        {
            if (is_array($value))
            {
                if(!isset($array2[$key]))
                {
                    $difference[$key] = $value;
                }
                elseif (!is_array($array2[$key]))
                {
                    $difference[$key] = $value;
                }
                else
                {
                    $new_diff = self::array_diff_assoc_recursive($value, $array2[$key]);
                    if ($new_diff != FALSE)
                    {
                        $difference[$key] = $new_diff;
                    }
                }
            }
            elseif (!isset($array2[$key]) || $array2[$key] != $value)
            {
                $difference[$key] = $value;
            }
        }
        return $difference;
    }
}