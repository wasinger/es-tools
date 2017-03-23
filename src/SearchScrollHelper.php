<?php
namespace Wa72\ESTools;
use Elasticsearch\Client;
use Psr\Log\LoggerAwareTrait;

/**
 * Helper class for scroll searches
 *
 * Class SearchScrollHelper
 * @package Wa72\ESTools
 */
class SearchScrollHelper
{
    use LoggerAwareTrait;

    /**
     * @var \Elasticsearch\Client
     */
    private $client;

    /**
     * @param \Elasticsearch\Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * Perform a search with scrolling
     *
     * @param string $index
     * @param array $options {
     *      @var mixed $query The query to pass to Elasticsearch, defaults to 'match_all'
     *      @var string $type The mapping type for Elasticsearch
     *      @var array|bool $_source The _source parameter for Elasticsearch. Set to FALSE if you only want a list of document IDs.
     *      @var int $size The size of the scroll window, default 100
     *      @var string $scroll The scroll timeout, default '10s'
     * }
     *
     * @return \Generator
     */
    public function scrollSearch($index, array $options = []): \Generator
    {
        $query = $options['query'] ?? ['match_all' => new \StdClass()];
        $_source = $options['_source'] ?? true;
        $type = $options['type'] ?? null;
        $size = $options['size'] ?? 100;
        $scroll = $options['scroll'] ?? '10s';

        $scroll_id = null;
        while (true) {
            if (!$scroll_id) { // first search request
                $search_request = [
                    "scroll" => $scroll,
                    "size" => $size,
                    'index' => $index,
                    'type' => $type,
                    'body' => [
                        'query' => $query,
                        '_source' => $_source,
                        'sort' => [
                            '_doc'
                        ]
                    ]
                ];
                $response = $this->client->search($search_request);
            } else { // Execute a Scroll request
                $response = $this->client->scroll([
                        "scroll_id" => $scroll_id,
                        "scroll" => $scroll
                    ]
                );
            }
            if ($this->logger) $this->logger->debug('Number of documents in current scroll: ' . count($response['hits']['hits']));
            if (count($response['hits']['hits']) > 0) {
                foreach ($response['hits']['hits'] as $hit) {
                    yield $hit;
                }
                $scroll_id = $response['_scroll_id'];
            } else {
                if ($this->logger) $this->logger->debug('No more results in current scroll.');
                break;
            }
        }
    }
}