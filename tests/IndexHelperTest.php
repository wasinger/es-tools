<?php
namespace Wa72\ESTools\tests;

use Wa72\ESTools\IndexHelper;

class IndexHelperTest extends ElasticsearchTestCase
{
    /**
     * @var IndexHelper
     */
    private $helper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->helper = new IndexHelper($this->client);
    }

    public function testCreateIndex()
    {
        $index = $this->prefix . 'firstindex';
        $created_index = $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index, $created_index, sprintf('Expected index name: %s, got %s', $index, $created_index));
        $this->waitForIndex($created_index);
        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $index]), sprintf('Index %s does not exist', $index));

        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));

        $new_index = $this->helper->createNewIndexVersion($index, $this->default_mapping, $this->default_settings);
        $this->assertEquals($index . '-1', $new_index, sprintf('Expected index name: %s, got: %s', $index . '-1', $new_index));
        $this->waitForIndex($new_index);
        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $new_index]), sprintf('Index %s does not exist', $new_index));

        $this->assertTrue($this->helper->isRealIndex($index));
//        $this->client->indices()->delete(['index' => $index]);

        $this->helper->switchAlias($index, $new_index);

        $this->assertFalse($this->helper->isRealIndex($index));
        $this->assertTrue($this->helper->isAlias($index));

        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $index]));

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
        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $index]), sprintf('Index %s does not exist', $index));
        $this->assertFalse($this->helper->isAlias($index));
        $this->assertEquals($index, $new_index, sprintf('Expected index name: %s, got %s', $index, $new_index));
        $this->assertEmpty($this->helper->diffIndexSettings($index, $this->default_settings));
        $this->assertEmpty($this->helper->diffMappings($index, $this->default_mapping));
        $this->assertEquals($aliases, $this->helper->getAliases($index));

        // index some data
        $this->client->index(['index' => $index] + ($this->usesMappingTypes() ? ['type' => '_doc'] : []) + ['body' => [
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
        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $index]), sprintf('Index %s does not exist', $index));
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
        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $index]));
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
        $this->assertTrue((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $new_index]));

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

    public function testNormalizeIndexSettingsWithSettingsKey()
    {
        $settings = ['settings' => ['index' => ['number_of_replicas' => 0], 'analysis' => ['analyzer' => []]]];
        $this->assertEquals(
            ['number_of_replicas' => 0, 'analysis' => ['analyzer' => []]],
            $this->helper->normalizeIndexSettings($settings)
        );
    }

    public function testCreateIndexWithAliasesAndWrappedMappings()
    {
        $index = $this->prefix . 'wrapped';
        $alias = $this->prefix . 'wrapped_alias';
        $created = $this->helper->createIndex(
            $index,
            ['mappings' => $this->default_mapping],
            ['settings' => $this->default_settings],
            [$alias]
        );
        $this->assertEquals($index, $created);
        $this->assertTrue($this->helper->exists($index));
        $this->assertTrue($this->helper->isRealIndex($index));
        $this->assertTrue($this->helper->isAlias($alias));
        $this->assertFalse($this->helper->isRealIndex($alias));
        $this->assertEquals([$alias], $this->helper->getAliases($index));
        $this->assertEmpty($this->helper->diffMappings($index, ['mappings' => $this->default_mapping]));
        $this->assertEmpty($this->helper->diffIndexSettings($index, ['settings' => $this->default_settings]));
    }

    public function testGetAliasesThrowsForAlias()
    {
        $index = $this->prefix . 'aliased';
        $alias = $this->prefix . 'the_alias';
        $this->helper->createIndex($index, [], [], [$alias]);
        $this->expectException(\InvalidArgumentException::class);
        $this->helper->getAliases($alias);
    }

    public function testDiffMappingsAndSettingsDetectChanges()
    {
        $index = $this->prefix . 'diff';
        $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);

        // a field that is not in the index mapping
        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $diff = $this->helper->diffMappings($index, $mapping);
        $this->assertEquals([($this->usesMappingTypes() ? '_doc.' : '') . 'properties.newfield' => ['type' => 'keyword']], $diff['-']);
        $this->assertArrayNotHasKey('+', $diff);

        // a field that is in the index but not in the given mapping
        $mapping = $this->default_mapping;
        unset($mapping['_doc']['properties']['tag']);
        $diff = $this->helper->diffMappings($index, $mapping);
        $this->assertEquals([($this->usesMappingTypes() ? '_doc.' : '') . 'properties.tag' => ['type' => 'keyword']], $diff['+']);
        $this->assertArrayNotHasKey('-', $diff);

        // changed analyzer settings
        $settings = $this->default_settings;
        $settings['analysis']['analyzer']['my_analyzer']['tokenizer'] = 'whitespace';
        $diff = $this->helper->diffIndexSettings($index, $settings);
        $this->assertEquals('whitespace', $diff['-']['analysis']['analyzer']['my_analyzer']['tokenizer']);
        $this->assertEquals('standard', $diff['+']['analysis']['analyzer']['my_analyzer']['tokenizer']);

        // no analysis settings given, but the index has some
        $diff = $this->helper->diffIndexSettings($index, []);
        $this->assertArrayHasKey('analysis', $diff['+']);

        // settings other than analysis and mapping are ignored
        $settings = $this->default_settings;
        $settings['number_of_replicas'] = 5;
        $this->assertEmpty($this->helper->diffIndexSettings($index, $settings));
    }

    public function testGenerateIncrementedIndexVersionName()
    {
        $this->assertEquals('myindex-1', $this->helper->generateIncrementedIndexVersionName('myindex', false));
        $this->assertEquals('myindex-1', $this->helper->generateIncrementedIndexVersionName('myindex', 'myindex'));
        $this->assertEquals('myindex-2', $this->helper->generateIncrementedIndexVersionName('myindex', 'myindex-1'));
        $this->assertEquals('myindex-11', $this->helper->generateIncrementedIndexVersionName('myindex', 'myindex-10'));
        $this->assertEquals('my-index-4', $this->helper->generateIncrementedIndexVersionName('my-index', 'my-index-3'));

        $this->expectException(\Exception::class);
        $this->helper->generateIncrementedIndexVersionName('myindex', 'otherindex-3');
    }

    public function testGetNextIndexVersionNameSkipsExistingIndices()
    {
        $index = $this->prefix . 'next';
        $this->assertEquals($index . '-1', $this->helper->getNextIndexVersionName($index));
        $this->helper->createIndex($index);
        $this->helper->createIndex($index . '-1');
        $this->helper->createIndex($index . '-2');
        $this->assertEquals($index . '-3', $this->helper->getNextIndexVersionName($index));
    }

    public function testGetCurrentIndexVersionNameForAliasWithMultipleIndices()
    {
        $alias = $this->prefix . 'multi';
        $this->helper->createIndex($alias . '-1', [], [], [$alias]);
        $this->helper->createIndex($alias . '-2', [], [], [$alias]);
        $this->assertFalse($this->helper->getCurrentIndexVersionName($alias));
        // switching is not possible if the alias points to more than one index
        $this->assertFalse($this->helper->switchAlias($alias, $alias . '-2'));
    }

    public function testSwitchAliasMovesAdditionalAliases()
    {
        $index = $this->prefix . 'switch';
        $other_alias = $this->prefix . 'switch_other';
        $additional_alias = $this->prefix . 'switch_additional';

        $this->helper->createIndex($index . '-1', [], [], [$index, $other_alias]);
        $this->helper->createIndex($index . '-2');

        $this->assertTrue($this->helper->switchAlias($index, $index . '-2', [$additional_alias]));

        $this->assertEquals($index . '-2', $this->helper->getCurrentIndexVersionName($index));
        $aliases = $this->helper->getAliases($index . '-2');
        sort($aliases);
        $expected = [$index, $additional_alias, $other_alias];
        sort($expected);
        $this->assertEquals($expected, $aliases);
        // the old index version has no aliases anymore, but still exists
        $this->assertEmpty($this->helper->getAliases($index . '-1'));
        $this->assertTrue($this->helper->isRealIndex($index . '-1'));
    }

    public function testSwitchAliasReplacesRealIndex()
    {
        $index = $this->prefix . 'replace';
        $other_alias = $this->prefix . 'replace_other';
        $this->helper->createIndex($index, [], [], [$other_alias]);
        $this->helper->createIndex($index . '-1');

        $this->assertTrue($this->helper->switchAlias($index, $index . '-1'));

        $this->assertTrue($this->helper->isAlias($index));
        $this->assertEquals($index . '-1', $this->helper->getCurrentIndexVersionName($index));
        // aliases of the replaced index are taken over
        $this->assertContains($other_alias, $this->helper->getAliases($index . '-1'));
    }

    public function testSetAliases()
    {
        $index = $this->prefix . 'setaliases';
        $this->helper->createIndex($index);
        $this->assertEquals([], $this->helper->setAliases($index, []));
        $aliases = [$this->prefix . 'a1', $this->prefix . 'a2'];
        $response = $this->helper->setAliases($index, $aliases);
        $this->assertTrue($response['acknowledged']);
        $this->assertEquals($aliases, $this->helper->getAliases($index));
    }

    public function testBulkIndex()
    {
        $index = $this->prefix . 'bulk';
        $helper = new IndexHelper($this->client, ['bulk_size' => 10]);
        $helper->createIndex($index, $this->default_mapping, $this->default_settings);

        // number of documents is a multiple of bulk_size
        $ids = $helper->bulkIndex($index, $this->generateDocuments(30), '_doc');
        $this->assertCount(30, $ids);
        $this->assertEquals('doc1', $ids[0]);
        $this->assertEquals('doc30', $ids[29]);
        $this->assertEquals(30, $this->countDocuments($index));

        // documents without _id get an auto-generated id
        $ids = $helper->bulkIndex($index, [['title' => 'no id 1'], ['title' => 'no id 2']], '_doc');
        $this->assertCount(2, $ids);
        $this->assertEquals(32, $this->countDocuments($index));

        // without default type, the type is taken from the _type key of the documents
        $ids = $helper->bulkIndex($index, [['_type' => '_doc', '_id' => 'typed', 'title' => 'with type']]);
        $this->assertEquals(['typed'], $ids);
        $this->assertEquals(33, $this->countDocuments($index));
        $this->assertEquals(['title' => 'with type'], $this->client->get(['index' => $index, 'id' => 'typed'] + ($this->usesMappingTypes() ? ['type' => '_doc'] : []))['_source']);

        // nothing to index
        $this->assertEquals([], $helper->bulkIndex($index, [], '_doc'));
    }

    public function testBulkIndexWithErrors()
    {
        $index = $this->prefix . 'bulkerrors';
        $this->helper->createIndex($index, [
            '_doc' => ['properties' => ['number' => ['type' => 'integer']]]
        ]);
        $docs = [
            ['_id' => 'ok1', 'number' => 1],
            ['_id' => 'bad', 'number' => 'not a number'],
            ['_id' => 'ok2', 'number' => 2],
        ];
        $ids = $this->helper->bulkIndex($index, $docs, '_doc');
        $this->assertEquals(['ok1', 'ok2'], $ids);
        $this->assertEquals(2, $this->countDocuments($index));
    }

    public function testReindex()
    {
        $src = $this->prefix . 'reindex_src';
        $dest = $this->prefix . 'reindex_dest';
        $this->helper->createIndex($src, $this->default_mapping, $this->default_settings);
        $this->helper->createIndex($dest, $this->default_mapping, $this->default_settings);
        $this->helper->bulkIndex($src, $this->generateDocuments(15), '_doc');
        // documents are reindexed even if the source index has not been refreshed yet
        $this->helper->reindex($src, $dest);
        $this->assertEquals(15, $this->countDocuments($dest));
    }

    public function testPrepareIndexReindexesData()
    {
        $index = $this->prefix . 'preparedata';
        $name = $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings, [], ['use_alias' => true]);
        $this->assertEquals($index . '-1', $name);
        $this->assertTrue($this->helper->isAlias($index));
        $this->helper->bulkIndex($index, $this->generateDocuments(20), '_doc');

        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $name = $this->helper->prepareIndex($index, $mapping, $this->default_settings, [], [
            'use_alias' => true,
            'reindex_data' => true,
        ]);
        $this->assertEquals($index . '-2', $name);
        $this->assertEquals($index . '-2', $this->helper->getCurrentIndexVersionName($index));
        $this->assertEquals(20, $this->countDocuments($index . '-2'));
        // old version is kept by default
        $this->assertTrue($this->helper->exists($index . '-1'));
    }

    public function testPrepareIndexDeletesOldVersion()
    {
        $index = $this->prefix . 'preparedelete';
        $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings, [], ['use_alias' => true]);
        $this->helper->bulkIndex($index, $this->generateDocuments(5), '_doc');

        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $name = $this->helper->prepareIndex($index, $mapping, $this->default_settings, [], [
            'use_alias' => true,
            'reindex_data' => true,
            'delete_old' => true,
        ]);
        $this->assertEquals($index . '-2', $name);
        $this->assertEquals(5, $this->countDocuments($index));
        $this->assertFalse($this->helper->exists($index . '-1'));
    }

    public function testPrepareIndexDeleteOldWithRealIndex()
    {
        // existing real index (not an alias) that is replaced by a new version
        $index = $this->prefix . 'preparereal';
        $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings);
        $this->assertTrue($this->helper->isRealIndex($index));
        $this->helper->bulkIndex($index, $this->generateDocuments(5), '_doc');

        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $name = $this->helper->prepareIndex($index, $mapping, $this->default_settings, [], [
            'use_alias' => true,
            'reindex_data' => true,
            'delete_old' => true,
        ]);
        $this->assertEquals($index . '-1', $name);
        $this->assertTrue($this->helper->isAlias($index));
        $this->assertEquals(5, $this->countDocuments($index));
    }

    public function testPrepareIndexWithoutReindex()
    {
        $index = $this->prefix . 'preparenoreindex';
        $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings, [], ['use_alias' => true]);
        $this->helper->bulkIndex($index, $this->generateDocuments(5), '_doc');

        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $name = $this->helper->prepareIndex($index, $mapping, $this->default_settings, [], ['use_alias' => true]);
        // new version is created, but the alias still points to the old version
        $this->assertEquals($index . '-2', $name);
        $this->assertEquals($index . '-1', $this->helper->getCurrentIndexVersionName($index));
        $this->assertEquals(0, $this->countDocuments($index . '-2'));
    }

    public function testPrepareIndexWithoutAliasReturnsFalseOnMismatch()
    {
        $index = $this->prefix . 'preparefalse';
        $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings);
        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $this->assertFalse($this->helper->prepareIndex($index, $mapping, $this->default_settings));
    }

    public function testIndexToNewIndexVersion()
    {
        $index = $this->prefix . 'tonew';
        $alias = $this->prefix . 'tonew_alias';
        $this->helper->prepareIndex($index, $this->default_mapping, $this->default_settings, [], ['use_alias' => true]);
        $this->helper->bulkIndex($index, $this->generateDocuments(3), '_doc');

        $new = $this->helper->indexToNewIndexVersion($this->generateDocuments(7), $index, $this->default_mapping, $this->default_settings, [$alias]);
        $this->assertEquals($index . '-2', $new);
        $this->assertEquals($new, $this->helper->getCurrentIndexVersionName($index));
        $this->assertContains($alias, $this->helper->getAliases($new));
        // the new index contains only the fresh documents
        $this->assertEquals(7, $this->countDocuments($new));
        // the old index version is untouched
        $this->assertEquals(3, $this->countDocuments($index . '-1'));
    }

    public function testCleanup()
    {
        $index = $this->prefix . 'cleanup';
        $this->helper->createIndex($index, $this->default_mapping, $this->default_settings);
        $this->helper->bulkIndex($index, $this->generateDocuments(25), '_doc');
        $this->refresh($index);

        $this->helper->cleanup($index, function ($hit) {
            return $hit['_source']['tag'] === 'even';
        });
        $this->assertEquals(12, $this->countDocuments($index));

        $result = $this->client->search(['index' => $index, 'body' => ['query' => ['term' => ['tag' => 'odd']]]]);
        $this->assertEquals(0, $this->totalHits($result));
    }
}
