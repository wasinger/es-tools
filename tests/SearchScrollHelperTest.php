<?php
namespace Wa72\ESTools\tests;

use Wa72\ESTools\IndexHelper;
use Wa72\ESTools\SearchScrollHelper;

class SearchScrollHelperTest extends ElasticsearchTestCase
{
    private $index;

    protected function setUp(): void
    {
        parent::setUp();
        $this->index = $this->prefix . 'scroll';
        $helper = new IndexHelper($this->client);
        $helper->createIndex($this->index, $this->default_mapping, $this->default_settings);
        $helper->bulkIndex($this->index, $this->generateDocuments(250), '_doc');
        $this->refresh($this->index);
    }

    public function testScrollSearchReturnsAllDocuments()
    {
        $sh = new SearchScrollHelper($this->client);
        $ids = [];
        foreach ($sh->scrollSearch($this->index, ['size' => 30]) as $hit) {
            $this->assertArrayHasKey('_source', $hit);
            $ids[] = $hit['_id'];
        }
        $this->assertCount(250, $ids);
        $this->assertCount(250, array_unique($ids));
    }

    public function testScrollSearchWithQueryTypeAndWithoutSource()
    {
        $sh = new SearchScrollHelper($this->client);
        $count = 0;
        foreach ($sh->scrollSearch($this->index, [
            'query' => ['term' => ['tag' => 'odd']],
            'type' => '_doc',
            '_source' => false,
            'size' => 50,
            'scroll' => '30s',
        ]) as $hit) {
            $this->assertArrayNotHasKey('_source', $hit);
            $this->assertEquals(1, substr($hit['_id'], 3) % 2);
            $count++;
        }
        $this->assertEquals(125, $count);
    }

    public function testScrollSearchWithoutResults()
    {
        $sh = new SearchScrollHelper($this->client);
        $hits = iterator_to_array($sh->scrollSearch($this->index, [
            'query' => ['term' => ['tag' => 'does-not-exist']]
        ]), false);
        $this->assertEquals([], $hits);
    }
    public function testScrollContextsAreReleasedAfterCompletionAndEarlyStop(): void
    {
        $openContexts = function (): int {
            return $this->client->indices()->stats(['index' => $this->index, 'metric' => 'search'])
                ['_all']['total']['search']['open_contexts'];
        };
        $this->assertSame(0, $openContexts());
        $helper = new SearchScrollHelper($this->client);
        iterator_to_array($helper->scrollSearch($this->index, ['size' => 100]), false);
        $this->assertSame(0, $openContexts());

        $scroll = $helper->scrollSearch($this->index, ['size' => 10]);
        $scroll->rewind();
        $this->assertGreaterThan(0, $openContexts());
        unset($scroll);
        $this->assertSame(0, $openContexts());
    }

}
