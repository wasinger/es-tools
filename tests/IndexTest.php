<?php
namespace Wa72\ESTools\tests;

use Wa72\ESTools\Index;

class IndexTest extends ElasticsearchTestCase
{
    private $aliases;

    protected function setUp(): void
    {
        parent::setUp();
        $this->aliases = [$this->prefix . 'index_alias'];
    }

    private function createIndexObject($mappings = null, $settings = null, $name = null): Index
    {
        return new Index(
            $this->client,
            $name ?? $this->prefix . 'index',
            $mappings ?? $this->default_mapping,
            $settings ?? $this->default_settings,
            $this->aliases
        );
    }

    private function createPreparedIndex(): Index
    {
        $index = $this->createIndexObject();
        $index->prepare();
        return $index;
    }

    public function testConstructorRejectsMultipleMappingTypes()
    {
        $this->expectException(\Exception::class);
        new Index($this->client, $this->prefix . 'index', [
            'type1' => ['properties' => []],
            'type2' => ['properties' => []],
        ], [], []);
    }

    public function testPrepareAndCheck()
    {
        $index = $this->createIndexObject();
        $this->assertEquals($this->prefix . 'index', $index->getName());
        $this->assertFalse($index->exists());
        $this->assertNull($index->checkSettingsAndMappings());
        $this->assertFalse($index->getRealIndexName());

        $real_name = $index->prepare();
        $this->assertEquals($this->prefix . 'index-1', $real_name);
        $this->assertTrue($index->exists());
        $this->assertEquals($real_name, $index->getRealIndexName());
        $this->assertEquals([], $index->checkSettingsAndMappings());
        $this->assertEquals([], $index->checkAliases());
        $this->assertSame($this->client, $index->getElasticsearchClient());
        $this->assertSame($this->client, $index->getClient());

        // calling prepare() again with the same settings does not create a new version
        $this->assertEquals($real_name, $index->prepare());
    }

    public function testPrepareWithWrappedMappingsAndSettings()
    {
        $index = $this->createIndexObject(['mappings' => $this->default_mapping], ['settings' => $this->default_settings]);
        $this->assertEquals($this->prefix . 'index-1', $index->prepare());
        $this->assertEquals([], $index->checkSettingsAndMappings());
    }

    public function testPrepareCreatesNewVersionOnMappingChangeAndKeepsData()
    {
        $index = $this->createPreparedIndex();
        $index->bulkIndex($this->generateDocuments(10));

        $mapping = $this->default_mapping;
        $mapping['_doc']['properties']['newfield'] = ['type' => 'keyword'];
        $changed = $this->createIndexObject($mapping);
        $this->assertArrayHasKey('mappings', $changed->checkSettingsAndMappings());

        $this->assertEquals($this->prefix . 'index-2', $changed->prepare());
        $this->assertEquals($this->prefix . 'index-2', $changed->getRealIndexName());
        $this->assertEquals([], $changed->checkSettingsAndMappings());
        $this->assertEquals([], $changed->checkAliases());
        $this->assertEquals(10, $this->countDocuments($changed->getName()));
        // Index::prepare() deletes old index versions
        $this->assertFalse((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $this->prefix . 'index-1']));
    }

    public function testCheckAndSetAliases()
    {
        $index = $this->createIndexObject();
        $index->prepare(false);
        $this->assertEquals($this->prefix . 'index', $index->getRealIndexName());
        $this->assertEquals([], $index->checkAliases());

        $this->client->indices()->deleteAlias(['index' => $this->prefix . 'index', 'name' => $this->aliases[0]]);
        $this->assertEquals($this->aliases, array_values($index->checkAliases()));
        $index->setAliases();
        $this->assertEquals([], $index->checkAliases());
    }

    public function testSwitchIndexVersion()
    {
        $index = $this->createPreparedIndex();
        $this->client->indices()->create(['index' => $this->prefix . 'index-5']);
        $this->assertTrue($index->switchIndexVersion($this->prefix . 'index-5'));
        $this->assertEquals($this->prefix . 'index-5', $index->getRealIndexName());
        $this->assertEquals([], $index->checkAliases());
    }

    public function testIndexGetAndDelete()
    {
        $index = $this->createPreparedIndex();

        $r = $index->index(['title' => 'First document', 'tag' => 'a'], 'id1');
        $this->assertEquals('created', $r['result']);
        $this->assertEquals('id1', $r['_id']);

        // id given as _id in data
        $r = $index->index(['_id' => 'id2', 'title' => 'Second document', 'tag' => 'b']);
        $this->assertEquals('id2', $r['_id']);

        // auto generated id
        $r = $index->index(['title' => 'Third document', 'tag' => 'c']);
        $this->assertNotEmpty($r['_id']);
        $id3 = $r['_id'];

        $doc = $index->get('id1');
        $this->assertTrue($doc['found']);
        $this->assertEquals('First document', $doc['_source']['title']);
        $this->assertEquals($this->prefix . 'index-1', $doc['_index']);

        $doc = $index->get('id2');
        $this->assertEquals(['title' => 'Second document', 'tag' => 'b'], $doc['_source']);
        $this->assertNull($index->get('does-not-exist'));

        $docs = $index->mget(['id1', $id3, 'does-not-exist']);
        $this->assertCount(3, $docs);
        $this->assertTrue($docs[0]['found']);
        $this->assertEquals('Third document', $docs[1]['_source']['title']);
        $this->assertFalse($docs[2]['found']);

        // update existing document
        $r = $index->index(['title' => 'First document updated', 'tag' => 'a'], 'id1');
        $this->assertEquals('updated', $r['result']);
        $this->assertEquals('First document updated', $index->get('id1')['_source']['title']);

        $index->delete('id1');
        $this->assertNull($index->get('id1'));
        // deleting a non existing document does not throw an exception
        $index->delete('does-not-exist');
        $this->assertEquals(2, $this->countDocuments($index->getName()));
    }

    public function testSearchFindOneByAndCount()
    {
        $index = $this->createPreparedIndex();
        $ids = $index->bulkIndex($this->generateDocuments(20));
        $this->assertCount(20, $ids);
        $this->refresh($index->getName());

        $this->assertEquals(20, $index->count());

        $result = $index->search(['query' => ['term' => ['tag' => 'odd']]]);
        $this->assertEquals(10, $this->totalHits($result));
        $this->assertCount(10, $result['hits']['hits']);

        $result = $index->search(['query' => ['match_all' => new \stdClass()]], [
            'size' => 5,
            'from' => 2,
            'sort' => [['sort_id' => 'asc']]
        ]);
        $this->assertEquals(20, $this->totalHits($result));
        $this->assertCount(5, $result['hits']['hits']);
        // sorted ids: doc1, doc10, doc11, doc12, doc13, ...
        $this->assertEquals('doc11', $result['hits']['hits'][0]['_id']);

        $hit = $index->findOneBy('_id', 'doc7');
        $this->assertEquals('doc7', $hit['_id']);
        $this->assertEquals('Document 7', $hit['_source']['title']);
        $this->assertNull($index->findOneBy('tag', 'does-not-exist'));
    }

    public function testCleanup()
    {
        $index = $this->createPreparedIndex();
        $index->bulkIndex($this->generateDocuments(10));
        $this->refresh($index->getName());

        // an empty list of existing ids does nothing
        $index->cleanup([]);
        $this->assertEquals(10, $this->countDocuments($index->getName()));

        $index->cleanup(['doc1', 'doc2', 'doc3']);
        $this->assertEquals(3, $this->countDocuments($index->getName()));
        $this->assertNotNull($index->get('doc2'));
        $this->assertNull($index->get('doc4'));
    }

    public function testDeleteIndex()
    {
        $index = $this->createPreparedIndex();
        $this->assertTrue($index->exists());
        $this->assertTrue($index->deleteIndex());
        $this->assertFalse($index->exists());
        $this->assertFalse((new \Wa72\ESTools\Internal\ClientAdapter($this->client))->indices('exists', ['index' => $this->prefix . 'index-1']));
    }

    public function testLogger()
    {
        $logger = new class extends \Psr\Log\AbstractLogger {
            public $records = [];
            public function log($level, $message, array $context = []): void
            {
                $this->records[] = [$level, (string) $message];
            }
        };
        $index = $this->createIndexObject();
        $index->setLogger($logger);
        $index->prepare();
        $index->index(['title' => 'Logged'], 'log1');
        $index->delete('log1');
        $messages = array_column($logger->records, 1);
        $this->assertNotEmpty(array_filter($messages, fn($m) => str_contains($m, 'does not exist, create it')));
        $this->assertNotEmpty(array_filter($messages, fn($m) => str_contains($m, 'successfully deleted')));
    }
}
