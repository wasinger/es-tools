<?php
namespace Wa72\ESTools\tests;

use Wa72\ESTools\Index;
use Wa72\ESTools\IndexHelper;

class CompatibilityTest extends ElasticsearchTestCase
{
    public function testTypelessMappingsWithRootOptionsAndArrayResponses(): void
    {
        $mappings = [
            'dynamic' => 'strict',
            '_source' => ['enabled' => true],
            'properties' => ['title' => ['type' => 'keyword']],
        ];
        $index = new Index($this->client, $this->prefix . 'typeless', $mappings, [], []);
        $this->assertFalse($index->exists());
        $this->assertSame($this->prefix . 'typeless-1', $index->prepare());
        $this->assertTrue($index->exists());
        $this->assertSame([], $index->checkSettingsAndMappings());
        $this->assertIsArray($index->index(['title' => 'zero'], 0));
        $this->assertSame('zero', $index->get(0)['_source']['title']);
        $this->assertSame('0', $index->get(0)['_id']);
        $this->assertIsArray($index->mget([0]));
        $this->refresh($index->getName());
        $this->assertIsArray($index->search(['query' => ['match_all' => new \stdClass()]]));
        $this->assertSame(1, $index->count());
        $index->delete(0);
        $this->assertNull($index->get(0));
        $index->delete(0);
        $this->assertTrue($index->deleteIndex());
        $this->assertFalse($index->exists());
    }

    public function testLegacyCustomTypeAndTypelessMappingsAreEquivalent(): void
    {
        $name = $this->prefix . 'custom_type';
        $properties = ['title' => ['type' => 'keyword']];
        $index = new Index($this->client, $name, ['article' => ['properties' => $properties]], [], []);
        $index->prepare();
        $this->assertSame(['one'], $index->bulkIndex([['_id' => 'one', '_type' => 'article', 'title' => 'First']]));
        $this->assertSame('First', $index->get('one')['_source']['title']);
        $helper = new IndexHelper($this->client);
        $this->assertSame([], $helper->diffMappings($name, ['article' => ['properties' => $properties]]));
        if (!$this->usesMappingTypes()) {
            $this->assertSame([], $helper->diffMappings($name, ['properties' => $properties]));
        }
    }

    public function testBulkIndexWithoutTypeAndWithRoutingAndZeroId(): void
    {
        $name = $this->prefix . 'routing';
        $helper = new IndexHelper($this->client, ['bulk_size' => 1]);
        $helper->createIndex($name, ['properties' => ['title' => ['type' => 'keyword']]]);
        $this->assertSame(['0', 'one'], $helper->bulkIndex($name, [
            ['_id' => 0, '_routing' => 'route-a', 'title' => 'Zero'],
            ['_id' => 'one', '_routing' => 'route-b', 'title' => 'One'],
        ]));
        $params = ['index' => $name, 'id' => 0, 'routing' => 'route-a'];
        if ($this->usesMappingTypes()) {
            $params['type'] = '_doc';
        }
        $this->assertSame(['title' => 'Zero'], $this->client->get($params)['_source']);
        $this->assertSame(2, $this->countDocuments($name));
    }

    public function testIndexToNewVersionWithTypelessMappings(): void
    {
        $name = $this->prefix . 'fresh_typeless';
        $helper = new IndexHelper($this->client);
        $mappings = ['properties' => ['title' => ['type' => 'keyword']]];
        $this->assertSame($name . '-1', $helper->indexToNewIndexVersion(
            [['_id' => 'one', 'title' => 'First']], $name, $mappings
        ));
        $this->assertSame(1, $this->countDocuments($name));
        $this->assertSame([], $helper->diffMappings($name, $mappings));
    }
    public function testGetPropagatesServerErrorsOtherThanNotFound(): void
    {
        $name = $this->prefix . 'blocked';
        $index = new Index($this->client, $name, [], [], []);
        $index->prepare(false);
        $index->index(['title' => 'Blocked'], 'one');
        $this->client->indices()->putSettings([
            'index' => $name, 'body' => ['index.blocks.read' => true],
        ]);
        $error = null;
        try {
            $index->get('one');
        } catch (\Exception $e) {
            $error = $e;
        } finally {
            $this->client->indices()->putSettings([
                'index' => $name, 'body' => ['index.blocks.read' => false],
            ]);
        }
        $this->assertNotNull($error, 'A read block must not be reported as a missing document.');
        $this->assertSame(403, $error->getCode());
    }

    public function testEmptyLegacyMappingsDoNotSendAnArrayAsTheMappingObject(): void
    {
        foreach ([['_doc' => []], ['mappings' => ['_doc' => []]]] as $i => $mappings) {
            $name = $this->prefix . 'empty_mapping_' . $i;
            $index = new Index($this->client, $name, $mappings, [], []);
            $this->assertSame($name . '-1', $index->prepare());
            $this->assertSame([], $index->checkSettingsAndMappings());
            $this->assertSame($name . '-1', $index->prepare());
        }
    }

}
