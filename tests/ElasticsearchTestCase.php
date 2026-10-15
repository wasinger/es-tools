<?php
namespace Wa72\ESTools\tests;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elastic\Elasticsearch\Client as ModernClient;
use OpenSearch\Client as OpenSearchClient;
use OpenSearch\GuzzleClientFactory;
use Elastic\Elasticsearch\ClientBuilder as ModernClientBuilder;
use Wa72\ESTools\Internal\ClientAdapter;
use PHPUnit\Framework\TestCase;

/**
 * Base class for tests that need a running Elasticsearch or OpenSearch server.
 *
 * The host can be configured with the environment variable ES_HOST (default: localhost:9200).
 * All indices created by tests must start with the prefix "estools_test_", they are removed before and after each test.
 */
abstract class ElasticsearchTestCase extends TestCase
{
    /**
     * @var Client|ModernClient|OpenSearchClient
     */
    protected $client;

    protected $prefix = 'estools_test_';

    protected $default_mapping;
    protected $default_settings;

    public static function createClient(): Client|ModernClient|OpenSearchClient
    {
        $host = getenv('ES_HOST') ?: 'localhost:9200';
        if (!str_contains($host, '://')) {
            $host = 'http://' . $host;
        }
        $engine = getenv('SEARCH_ENGINE') ?: 'elasticsearch';
        if ($engine === 'opensearch') {
            return (new GuzzleClientFactory())->create(['base_uri' => $host]);
        }
        if ($engine !== 'elasticsearch') {
            throw new \InvalidArgumentException('Unknown SEARCH_ENGINE: ' . $engine);
        }
        $builder = class_exists(ModernClientBuilder::class) ? ModernClientBuilder::create() : ClientBuilder::create();
        return $builder->setHosts([$host])->build();
    }

    protected function setUp(): void
    {
        $this->client = static::createClient();
        try {
            $available = (new ClientAdapter($this->client))->request('ping');
        } catch (\Exception $e) {
            $available = false;
        }
        if (!$available) {
            $message = (getenv('SEARCH_ENGINE') ?: 'elasticsearch') . ' is not available at ' . (getenv('ES_HOST') ?: 'localhost:9200');
            if (getenv('CI')) {
                // never silently skip tests in CI
                $this->fail($message);
            }
            $this->markTestSkipped($message);
        }
        // make sure test indices do not exist already
        $this->deleteTestIndices();
        $this->client->cluster()->health([
            'wait_for_status' => 'yellow',
            'timeout' => '10s'
        ]);

        $this->default_settings = [
            "analysis" => [
                "analyzer" => [
                    "my_analyzer" => [
                        "type" => "custom",
                        "tokenizer" => "standard",
                        "char_filter" => ["html_strip"],
                        "filter" => ["lowercase", "asciifolding"]
                    ]
                ]
            ]
        ];

        $this->default_mapping = [
            "_doc" => [
                "properties" => [
                    "title" => [
                        "type" => "text",
                        "analyzer" => "my_analyzer"
                    ],
                    "sort_id" => ["type" => "keyword"],
                    "tag" => [
                        "type" => "keyword"
                    ]
                ]
            ]
        ];
    }

    protected function tearDown(): void
    {
        // delete all indices created during tests
        if ($this->client) {
            $this->deleteTestIndices();
        }
    }

    protected function deleteTestIndices(): void
    {
        try {
            $this->client->indices()->delete(['index' => $this->prefix . '*']);
        } catch (\Exception $e) {
        }
    }

    protected function usesMappingTypes(): bool
    {
        return (new ClientAdapter($this->client))->usesMappingTypes();
    }

    protected function totalHits($result): int
    {
        $total = $result['hits']['total'];
        return is_array($total) ? $total['value'] : $total;
    }

    protected function refresh(string $index): void
    {
        $this->client->indices()->refresh(['index' => $index]);
    }

    protected function countDocuments(string $index): int
    {
        $this->refresh($index);
        return $this->client->count(['index' => $index])['count'];
    }

    /**
     * Generate test documents
     */
    protected function generateDocuments(int $count, bool $with_id = true): \Generator
    {
        for ($i = 1; $i <= $count; $i++) {
            $doc = [
                'title' => 'Document ' . $i,
                'sort_id' => 'doc' . $i,
                'tag' => $i % 2 ? 'odd' : 'even',
            ];
            if ($with_id) {
                $doc['_id'] = 'doc' . $i;
            }
            yield $doc;
        }
    }

    protected function waitForIndex($indexName)
    {
        $maxWaitSeconds = 20;
        $startTime = time();

        while (time() - $startTime < $maxWaitSeconds) {
            try {
                $health = $this->client->cluster()->health([
                    'index' => $indexName,
                    'wait_for_status' => 'yellow',
                    'timeout' => '5s'
                ]);
                if (empty($health['timed_out'])) {
                    return true;
                }
            } catch (\Exception $e) {
                // index not ready yet, continue waiting
            }
            usleep(200000);
        }
        throw new \Exception("Index '$indexName' was not ready after $maxWaitSeconds seconds");
    }
}
