<?php
namespace Wa72\ESTools\tests;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
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
        $this->client->indices()->delete(['index' => $this->prefix . '*']);
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
        $this->client->indices()->delete(['index' => $this->prefix . '*']);
    }

    public function testCreateIndex()
    {
        $index = $this->prefix . 'firstindex';
        $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);

        $this->assertTrue($this->client->indices()->exists(['index' => $index]));

        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));

        $new_index = $this->helper->createNewIndexVersion($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index . '-1', $new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $new_index]));

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
        $this->assertTrue($this->client->indices()->exists(['index' => $index]));
        $this->assertFalse($this->helper->isAlias($index));
        $this->assertEquals($index, $new_index);
        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));
        $this->assertEquals($aliases, $this->helper->getAliases($index));

        // index some data
        $this->client->index(['index' => $index, 'type' => '_doc', 'body' => [
            'title' => 'First Document',
            'another_field' => 'more content'
        ]]);

        $new_index = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index, $new_index); // The index should not have changed

        // change mapping and create a new index version
        $mapping['_doc']['properties']['keywords'] = ['type' => 'keyword'];
        $new_index = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index . '-1', $new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]));
        $this->assertTrue($this->helper->isAlias($index));
        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertEquals($new_index, $this->helper->getCurrentIndexVersionName($index));
        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $mapping));
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
        $this->assertEquals($index . '-2', $new_index1);
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
        // the old index version should not have aliases any more
        $this->assertEmpty($this->helper->getAliases($new_index));
    }

    public function testGetCurrentVersionIndexName()
    {
        $index = $this->prefix . 'thirdindex';

        // $index does not exist
        $this->assertEquals(false, $this->helper->getCurrentIndexVersionName($index));

        $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);
        $new_index = $this->helper->createNewIndexVersion($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index . '-1', $new_index);
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
}
