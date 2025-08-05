<?php
namespace Wa72\ESTools;
use Elasticsearch\Client;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Wa72\ESTools\IndexHelper;

class Index
{
    use LoggerAwareTrait;

    /**
     * @var Client
     */
    private $es;
    /**
     * @var IndexHelper
     */
    private $indexhelper;

    private $index_name;

    private $settings;
    private $mappings;
    private $aliases;

    private $type = '_doc';

    public function __construct(Client $es_client, string $index_name, $mappings, $settings, $aliases)
    {
        $this->es = $es_client;
        $this->indexhelper = new IndexHelper($this->es);
        $this->index_name = $index_name;
        $this->mappings = $mappings;
        $this->settings = $settings;
        $this->aliases = $aliases;
        if (isset($mappings['mappings'])) {
            $mappings = $mappings['mappings'];
        }
        if (!empty($mappings)) {
            if (count($mappings) == 1) {
                // Get Type from mappings array
                $this->type = array_keys($mappings)[0];
            } else {
                // In preparation for ES 6 we support only one type per index
                throw new \Exception('Mappings array is supposed to contain exactly one element the key of which is the mapping type.');
            }
        }
    }

    public function getName()
    {
        return $this->index_name;
    }

    /**
     * @return bool
     */
    public function exists()
    {
        return $this->indexhelper->exists($this->index_name);
    }

    /**
     *
     * @return array|null Return null if index does not exist, empty array if mappings and settings match, array differences otherwise
     */
    public function checkSettingsAndMappings()
    {
        if (!$this->exists()) return null;
        $a = [];
        $diff_settings = $this->indexhelper->diffIndexSettings($this->index_name, $this->settings);
        if (!empty($diff_settings)) {
            $a['settings'] = $diff_settings;
        }
        $diff_mappings = $this->indexhelper->diffMappings($this->index_name, $this->mappings);
        if (!empty($diff_mappings)) {
            $a['mappings'] = $diff_mappings;
        }
        return $a;
    }
    public function getRealIndexName()
    {
        return $this->indexhelper->getCurrentIndexVersionName($this->index_name);
    }

    /**
     * Switch alias to a another index version
     *
     * This index version must already exist, e.g. created by $this->prepare(true, false)
     */
    public function switchIndexVersion($realIndexVersionName)
    {
        return $this->indexhelper->switchAlias($this->index_name, $realIndexVersionName, $this->aliases);
    }

    public function checkAliases()
    {
        $current_index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $aliases = $this->indexhelper->getAliases($current_index);
        return array_diff($this->aliases, $aliases);
    }

    /**
     * @return array
     */
    public function setAliases()
    {
        return $this->indexhelper->setAliases($this->indexhelper->getCurrentIndexVersionName($this->index_name), $this->aliases);
    }

    public function prepare($use_alias = true, $reindex_data = true)
    {
        $index = $this->indexhelper->prepareIndex(
            $this->index_name,
            $this->mappings,
            $this->settings,
            $this->aliases,
            [
                'use_alias' => $use_alias,
                'reindex_data' => $reindex_data,
                'delete_old' => true
            ]
        );
        return $index;
    }

    /**
     * Entfernt alle Dokumente aus dem Index,
     * die nicht in $existing_ids gelistet sind
     *
     * @param array $existing_ids Array von Dokument-IDs
     */
    public function cleanup($existing_ids)
    {
        if (count($existing_ids)) {
            $this->indexhelper->cleanup($this->index_name, function ($hit) use ($existing_ids) {
                if (in_array($hit['_id'], $existing_ids)) {
                    return true;
                }
                return false;
            }, [
                '_source' => false
            ]);
        }
    }

    /**
     * Indiziere einen Datensatz
     *
     * @param array $data
     * @param string|int|null $id The ID of the record
     * @return array The response from ES
     * @throws \Exception
     */
    public function index(array $data, $id = null)
    {
        if (isset($data['_id'])) {
            if ($id === null) $id = $data['_id'];
            unset($data['_id']);
        }
        $this->log(LogLevel::DEBUG, sprintf('index: indexing %s/%s', $this->index_name, $id));
        $params = array_filter([
            'index' => $this->index_name,
            'type' => $this->type,
            'id' => $id,
            'ttl' => isset($data['_ttl']) ? $data['_ttl'] : null,
            'routing' => isset($data['_routing']) ? $data['_routing'] : null,
            'parent' => isset($data['_parent']) ? $data['_parent'] : null,
        ]);
        unset($data['_type'], $data['_ttl'], $data['_routing'], $data['_parent']);
        $params['body'] = $data;
        $r = $this->es->index($params);
        return $r;
    }

    /**
     * Delete one document by id
     *
     * @param $id
     * @throws \Exception
     */
    public function delete($id)
    {
        $index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $params = [
            'index' => $index,
            'type' => $this->type,
            'id' => $id
        ];

        $r = $this->es->delete($params);

//        // Example responses:
//        1. Successfully deleted:
//        {
//            "found": true,
//            "_index": "index-6",
//            "_type": "doc",
//            "_id": "8830",
//            "_version": 17,
//            "result": "deleted",
//            "_shards": {
//                "total": 2,
//                "successful": 1,
//                "failed": 0
//            }
//        }
//
//        2. Not found:
//        {
//            "found": false,
//            "_index": "index-6",
//            "_type": "doc",
//            "_id": "8830",
//            "_version": 3,
//            "result": "not_found",
//            "_shards": {
//                "total": 2,
//                "successful": 1,
//                "failed": 0
//            }
//        }

        // We accept "not_found" as a successful result for the delete operation
        if ($r['result'] == 'deleted' || $r['result'] == 'not_found') {
            $this->log(LogLevel::DEBUG, sprintf('Document %s/%s/%s successfully deleted.', $index, $this->type, $id));
        } else {
            $this->log(LogLevel::ERROR, sprintf('Document %s/%s/%s could not be deleted.', $index, $this->type, $id));
            throw new \Exception(sprintf('Document %s/%s/%s could not be deleted.', $index, $this->type, $id));
        }

    }

    /**
     * Get a document by ID
     *
     * @param mixed $id
     * @return mixed|null
     */
    public function get($id): ?array
    {
        $index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $params = [
            'index' => $index,
            'type' => $this->type,
            'id' => $id
        ];
        try {
            $r = $this->es->get($params);
            return $r;
        } catch (\Exception $e) {
            // Document not found
            return null;
        }
    }

    /**
     * Get multiple documents by their IDs
     *
     * @param array $ids
     * @return array
     */
    public function mget(array $ids): array
    {
        $index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $params = [
            'index' => $index,
            'type' => $this->type,
            'body' => [
                'ids' => $ids
            ]
        ];
        try {
            $r = $this->es->mget($params);
            return $r['docs'];
        } catch (\Exception $e) {
            // Document not found
            return [];
        }
    }

    /**
     * Search for documents in the index
     *
     * @param array $query The query definition, e.g. ['query' => ['match' => ['field' => 'value']]]. Must begin with 'query' key.
     * @param array $options Additional options 'size', 'from', 'sort'
     * @return array The search results
     */
    public function search(array $query, array $options = []): array
    {
        $index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $params = [
            'index' => $index,
            'type' => $this->type,
            'body' => $query
        ];
        if (isset($options['size'])) {
            $params['body']['size'] = $options['size'];
        }
        if (isset($options['from'])) {
            $params['body']['from'] = $options['from'];
        }
        if (isset($options['sort'])) {
            $params['body']['sort'] = $options['sort'];
        }
        return $this->es->search($params);
    }

    public function findOneBy($field, $value)
    {
        $index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $params = [
            'index' => $index,
            'type' => $this->type,
            'body' => [
                'query' => [
                    'term' => [
                        $field => $value
                    ]
                ],
                'size' => 1
            ]
        ];
        $result = $this->es->search($params);
        if (isset($result['hits']['hits']) && count($result['hits']['hits']) > 0) {
            return $result['hits']['hits'][0]['_source'];
        }
        return null;
    }

    /**
     * @param iterable $data
     * @return array Array of IDs of indexed documents
     */
    public function bulkIndex(iterable $data) {
        return $this->indexhelper->bulkIndex($this->index_name, $data);
    }

    public function deleteIndex(): bool
    {
        $params = ['index' => $this->getRealIndexName()];
        $response = $this->es->indices()->delete($params);
        if (!empty($response['acknowledged'])) {
            $this->log(LogLevel::DEBUG, sprintf('Index %s successfully deleted.', $this->getRealIndexName()));
            return true;
        } else {
            if (!empty($response['error'])) {
                $this->log(LogLevel::ERROR, sprintf('Error deleting index %s: %s', $this->getRealIndexName(), $response['error']['reason']));
            }
            $this->log(LogLevel::ERROR, sprintf('Index %s could not be deleted.', $this->getRealIndexName()));
            return false;
        }
    }

    public function count(): int|false
    {
        $index = $this->indexhelper->getCurrentIndexVersionName($this->index_name);
        $res = $this->es->count(['index' => $index]);
        if (isset($res['count'])) {
            return $res['count'];
        }
        return false;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->indexhelper->setLogger($logger);
    }

    public function getElasticsearchClient(): Client
    {
        return $this->es;
    }

    private function log($level, $message)
    {
        if ($this->logger instanceof LoggerInterface) {
            $this->logger->log($level, $message);
        }
    }



}