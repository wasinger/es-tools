<?php
namespace Wa72\ESTools\tests;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Wa72\ESTools\IndexHelper;

class IndexHelperTest extends \PHPUnit_Framework_TestCase
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


    public function setUp()
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
            "my_type" => [
                "properties" => [
                    "title" => [
                        "type" => "text",
                        "analyzer" => "my_analyzer"
                    ]
                ]
            ]
        ];
    }

    public function tearDown()
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
        $this->assertEquals($index . '-0', $new_index);
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

        // change mapping and create a new index version
        $mapping['my_type']['properties']['keywords'] = ['type' => 'keyword'];
        $new_index = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index . '-0', $new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]));
        $this->assertTrue($this->helper->isAlias($index));
        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertEquals($new_index, $this->helper->getCurrentIndexVersionName($index));
        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $mapping));
        $this->assertArraySubset($aliases, $this->helper->getAliases($new_index));

        // change mapping again and create a new index version
        $mapping['my_type']['properties']['more_keywords'] = ['type' => 'keyword'];
        $new_index1 = $this->helper->prepareIndex($index, $mapping, $this->default_settings, $aliases, [
            'use_alias' => true,
            'reindex_data' => true
        ]);
        $this->assertEquals($index . '-1', $new_index1);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]));
        $this->assertTrue($this->helper->isAlias($index));
        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertEquals($new_index1, $this->helper->getCurrentIndexVersionName($index));
        $this->assertEmpty($this->helper->diffIndexSettings($new_index1, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($new_index1, $mapping));
        $this->assertArraySubset($aliases, $this->helper->getAliases($new_index1));
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
        $this->assertEquals($index . '-0', $new_index);
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
