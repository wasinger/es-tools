<?php
namespace Wa72\ESTools\tests;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use PHPUnit\Framework\TestCase;
use Wa72\ESTools\IndexHelper;

class IndexHelperTest extends TestCase
{
    /**
     * @var Client
     */
    private $client;
    /**
     * @var IndexHelper
     */
    private $helper;

    private $prefix = 'estools_test_';

    private $default_mapping;
    private $default_settings;


    public function setUp(): void
    {
        $this->client = ClientBuilder::create()->build();
        // make sure test indices do not exist already
        try {
            $this->client->indices()->delete(['index' => $this->prefix . '*']);
        } catch (\Exception $e) {
        }
        $this->client->cluster()->health([
            'wait_for_status' => 'yellow',
            'timeout' => '10s'
        ]);
        $this->helper = new IndexHelper($this->client);
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
                    ]
                ]
            ]
        ];
    }

    public function tearDown(): void
    {
        // delete all indices created during tests
        try {
            $this->client->indices()->delete(['index' => $this->prefix . '*']);
        } catch (\Exception $e) {
        }
    }

    public function testCreateIndex()
    {
        $index = $this->prefix . 'firstindex';
        $created_index = $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index, $created_index, sprintf('Expected index name: %s, got %s', $index, $created_index));
        $this->waitForIndex($created_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]), sprintf('Index %s does not exist', $index));

        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));

        $new_index = $this->helper->createNewIndexVersion($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index . '-1', $new_index, sprintf('Expected index name: %s, got: %s', $index . '-1', $new_index));
        $this->waitForIndex($new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $new_index]), sprintf('Index %s does not exist', $new_index));

        $this->assertTrue($this->helper->isRealIndex($index));
//        $this->client->indices()->delete(['index' => $index]);

        $this->helper->switchAlias($index, $new_index);

        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertTrue($this->helper->isAlias($index));

        $this->assertTrue($this->client->indices()->exists(['index' => $index]));

        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));
    }

    public function testPrepareIndex()
    {
        $index = $this->prefix . 'secondindex';
        $mapping = $this->default_mapping;
        $aliases = [$this->prefix . 'firstalias', $this->prefix . 'secondalias'];

        // create a real index
        $new_index = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => false
        ]);
        $this->waitForIndex($new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]), sprintf('Index %s does not exist', $index));
        $this->assertFalse($this->helper->isAlias($index));
        $this->assertEquals($index, $new_index, sprintf('Expected index name: %s, got %s', $index, $new_index));
        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));
        $this->assertEquals($aliases, $this->helper->getAliases($index));

        // index some data
        $this->client->index(['index' => $index, 'type' => '_doc', 'body' => [
            'title' => 'First Document'
        ]]);

        $new_index = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index, $new_index, sprintf('Expected index name: %s, got %s', $index, $new_index)); // The index should not have changed
        $this->waitForIndex($new_index);
        // change mapping and create a new index version
        $mapping['_doc']['properties']['keywords'] = ['type' => 'keyword'];
        $new_index = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index . '-1', $new_index, sprintf('Expected index name: %s, got %s', $index . '-1', $new_index));
        $this->waitForIndex($new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]), sprintf('Index %s does not exist', $index));
        $this->assertTrue($this->helper->isAlias($index));
        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertEquals($new_index, $this->helper->getCurrentIndexVersionName($index));
        $diff_settings = $this->helper->diffIndexSettings($index, $this->default_settings);
        $this->assertEmpty($diff_settings, 'Settings differ: ' . json_encode($diff_settings));
        $diff_mappings = $this->helper->diffMappings($index, $mapping);
        $this->assertEmpty($diff_mappings, 'Mappings differ: ' . json_encode($diff_mappings));
        #$this->assertArraySubset($aliases, $this->helper->getAliases($new_index)); # deprecated in phpunit 8
        $current_aliases = $this->helper->getAliases($new_index);
        foreach($aliases as $alias) {
            $this->assertContains($alias, $current_aliases);
        }

        // change mapping again and create a new index version
        $mapping['_doc']['properties']['more_keywords'] = ['type' => 'keyword'];
        $new_index1 = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index . '-2', $new_index1, sprintf('Expected index name: %s, got %s', $index . '-2', $new_index1));
        $this->waitForIndex($new_index1);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]));
        $this->assertTrue($this->helper->isAlias($index));
        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertEquals($new_index1, $this->helper->getCurrentIndexVersionName($index));
        $this->assertEmpty($this->helper->diffIndexSettings($new_index1, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($new_index1, $mapping));
        #$this->assertArraySubset($aliases, $this->helper->getAliases($new_index1)); # deprecated in phpunit 8
        $current_aliases = $this->helper->getAliases($new_index1);
        foreach($aliases as $alias) {
            $this->assertContains($alias, $current_aliases);
        }
        // the old index version should not have aliases anymore
        $this->assertEmpty($this->helper->getAliases($new_index));
    }

    public function testGetCurrentVersionIndexName()
    {
        $index = $this->prefix . 'thirdindex';

        // $index does not exist
        $currentVersionName = $this->helper->getCurrentIndexVersionName($index);
        $this->assertEquals(false, $currentVersionName, sprintf('Index %s must not exist', $currentVersionName));

        $created_index = $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index, $created_index, sprintf('Expected index name: %s, got %s', $index, $created_index));
        $this->waitForIndex($created_index);
        $new_index = $this->helper->createNewIndexVersion($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index . '-1', $new_index, sprintf('Expected index name: %s, got %s', $index . '-1', $new_index));
        $this->waitForIndex($new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $new_index]));

        // alias $index for $new_index not set yet
        $this->assertEquals($index, $this->helper->getCurrentIndexVersionName($index));

        $this->helper->switchAlias($index, $new_index);

        $this->assertEquals($new_index, $this->helper->getCurrentIndexVersionName($index));
    }

    public function testNormalizeIndexSettings()
    {
        $settings = [
            "index" => [
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
            ],
            "index.mapping.single_type" => true
        ];

        $normalized = [
            "analysis" => [
                "analyzer" => [
                    "my_analyzer" => [
                        "type" => "custom",
                        "tokenizer" => "standard",
                        "char_filter" => ["html_strip"],
                        "filter" => ["lowercase", "asciifolding"]
                    ]
                ]
            ],
            "mapping" => [
                "single_type" => true
            ]
        ];

        $this->assertEquals($normalized, $this->helper->normalizeIndexSettings($settings));
    }
    private function waitForIndex($indexName)
    {
        $maxWaitSeconds = 20;
        $startTime = time();

        while (time() - $startTime < $maxWaitSeconds) {
            try {
                if ($this->client->indices()->exists(['index' => $indexName])) {
                    $stats = $this->client->indices()->stats(['index' => $indexName]);
                    if (isset($stats['indices'][$indexName])) {
                        return true;
                    }
                }
            } catch (\Exception $e) {
                // Index noch nicht bereit, weiter warten
            }
            sleep(1);
        }
        throw new \Exception("Index '$indexName' was not ready after $maxWaitSeconds seconds");
    }
}
