<?php
namespace Wa72\ESTools;

use Elasticsearch\Client;
use Psr\Log\LoggerAwareTrait;

/**
 * This class contains some convenience functions for working with Elasicsearch indices
 */
class IndexHelper
{
    use LoggerAwareTrait;

    /**
     * @var \Elasticsearch\Client
     */
    private $client;

    /**
     * @var string
     */
    private $index;

    /**
     * @param \Elasticsearch\Client $client
     * @param string $index The name of the index to operate on
     */
    public function __construct(Client $client, string $index)
    {
        $this->client = $client;
        $this->index = $index;
    }

    /**
     * Cleanup index.
     *
     * This function lists all documents in the index and passes them to a "voter" function.
     * If this function returns TRUE the document is kept, if it returns FALSE the document
     * will be removed from the index.
     *
     * @param callable $voter A function which accepts an ES document
     * @param array $scroll_options Options passed to SearchScrollHelper::scrollSearch
     */
    public function cleanup(callable $voter, array $scroll_options)
    {
        if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::cleanup(): Checking for documents to delete from index...');
        $scrollhelper = new SearchScrollHelper($this->client);
        if ($this->logger) $scrollhelper->setLogger($this->logger);
        $counter = 0;
        foreach ($scrollhelper->scrollSearch($this->index, $scroll_options) as $hit) {
            $id = $hit['_id'];
            $type = $hit['_type'];
            $index = $hit['_index'];
            if (!call_user_func_array($voter, [&$hit])) {
                $params = [
                    'index' => $index,
                    'type' => $type,
                    'id' => $id
                ];
                $this->client->delete($params);
                $counter++;
                if ($this->logger) $this->logger->debug(sprintf('Wa72\ESTools\IndexHelper::cleanup(): deleted %s/%s/%s', $index, $type, $id));
            }
        }
        if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::cleanup(): ' . $counter . ' documents deleted from index.');
    }
}