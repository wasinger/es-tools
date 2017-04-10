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
     * @var array
     */
    private $index_aliases = [];

    /**
     * @var array
     */
    private $analysis = [];

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
     * Clean up index.
     *
     * This function lists all documents in the index and passes them to a "voter" function.
     * If this function returns TRUE the document is kept, if it returns FALSE the document
     * will be removed from the index.
     *
     * @param callable $voter A function which accepts an ES document as parameter and returns TRUE or FALSE
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

    /**
     * Prüfe, ob Index schon vorhandenen ist,
     * und erstelle Mapping für den Type
     *
     * @param string $type
     * @param array $mapping
     */
    public function check_index($type, $mapping = [])
    {
        // check whether index exists, if not create it
        if (!$this->client->indices()->exists(['index' => $this->index])) {
            if ($this->logger) $this->logger->info('Wa72\ESTools\IndexHelper::check_index: index ' . $this->index . " does not exist, create it");
            $body = [];
            if (!empty($this->analysis)) {
                $body['settings'] = [
                    'analysis' => $this->analysis
                ];
            }
            if (!empty($mapping)) {
                $body['mappings'] = [
                    $type => $mapping
                ];
            }
            $params = [
                'index' => $this->index
            ];
            if (!empty($body)) {
                $params['body'] = $body;
            }
            $this->client->indices()->create($params);
            $alias_actions = [];
            if (count($this->index_aliases)) {
                foreach ($this->index_aliases as $alias) {
                    $alias_actions[] = ['add' => ['index' => $this->index, 'alias' => $alias]];
                }
                $this->client->indices()->updateAliases([
                    'index' => $this->index,
                    'body' => [
                        'actions' => $alias_actions
                    ]
                ]);
            }
        } elseif (!empty($mapping)) {
            // Wiederholtes putMapping ist kein Problem,
            // solange es keine Konflikte zu vorherigen Mappings
            // sowie Mappings von anderen Types gibt.
            // Im Falle eine Konfliktes gibt es eine Exception.
            $this->client->indices()->putMapping([
                'index' => $this->index,
                'type' => $type,
                'body' => [
                    $type => $mapping
                ]
            ]);
        }
    }

    /**
     * Set Aliases for the current index
     *
     * @param array $index_aliases
     */
    public function setIndexAliases(array $index_aliases)
    {
        $this->index_aliases = $index_aliases;
    }

    /**
     * @return array
     */
    public function getAnalysis(): array
    {
        return $this->analysis;
    }

    /**
     * @param array $analysis
     */
    public function setAnalysis(array $analysis)
    {
        $this->analysis = $analysis;
    }



}