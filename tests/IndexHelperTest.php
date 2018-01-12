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

        $this->assertEmpty($this->helper->diffAnalysis($index, $this->default_settings['analysis']));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));

        $new_index = $this->helper->createNewIndexVersion($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index . '-0', $new_index);
        $this->assertTrue($this->client->indices()->exists(['index' => $new_index]));

        $this->assertTrue($this->helper->isRealIndex($index));
//        $this->client->indices()->delete(['index' => $index]);

        $this->helper->setAliases($new_index, [$index], true);

        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertTrue($this->helper->isAlias($index));

        $this->assertTrue($this->client->indices()->exists(['index' => $index]));

        $this->assertEmpty($this->helper->diffAnalysis($index, $this->default_settings['analysis']));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));


    }

    public function testPrepareIndex()
    {
        $index = $this->prefix . 'secondindex';

        $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings);
        $this->assertTrue($this->client->indices()->exists(['index' => $index]));

        $this->assertEmpty($this->helper->diffAnalysis($index, $this->default_settings['analysis']));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));

    }
}
