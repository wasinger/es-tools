<?php
namespace Wa72\ESTools\tests;

use Elastic\Elasticsearch\Client as ModernClient;
use Elasticsearch\Client;
use OpenSearch\Client as OpenSearchClient;
use Wa72\ESTools\Index;
use Wa72\ESTools\Internal\ClientAdapter;

class ClientSelectionTest extends ElasticsearchTestCase
{
    public function testNativeClientAndAdapterMatchTheSelectedEngine(): void
    {
        $adapter = new ClientAdapter($this->client);
        $this->assertSame($this->client, $adapter->getClient());
        $info = $adapter->request('info');
        if (getenv('EXPECTED_SEARCH_VERSION')) {
            $this->assertSame(getenv('EXPECTED_SEARCH_VERSION'), $info['version']['number']);
        }
        $index = new Index($this->client, $this->prefix . 'selected_client', [], [], []);
        $this->assertSame($this->client, $index->getClient());
        $this->assertSame($this->client, $index->getElasticsearchClient());

        if (getenv('SEARCH_ENGINE') === 'opensearch') {
            $this->assertInstanceOf(OpenSearchClient::class, $index->getClient());
            $this->assertFalse($adapter->usesMappingTypes());
            $this->assertSame('opensearch', $info['version']['distribution']);
        } else {
            $this->assertTrue($this->client instanceof Client || $this->client instanceof ModernClient);
        }
    }

    public function testMissingResourcesAreHandledWithTheNativeClient(): void
    {
        $adapter = new ClientAdapter($this->client);
        $name = $this->prefix . 'missing_resources';
        $this->assertFalse($adapter->indices('exists', ['index' => $name]));
        $this->assertFalse($adapter->indices('existsAlias', ['name' => $name]));
        $index = new Index($this->client, $name, [], [], []);
        $this->assertFalse($index->getRealIndexName());
        $index->prepare();
        $this->assertNull($index->get('missing'));
        $index->delete('missing');
        $documents = $index->mget(['missing']);
        $this->assertCount(1, $documents);
        $this->assertFalse($documents[0]['found']);
    }
    public function testZeroIdSupportsUpdatesAndPropagatesIndexingErrors(): void
    {
        $index = new Index($this->client, $this->prefix . 'zero_id', [
            'dynamic' => 'strict',
            'properties' => ['title' => ['type' => 'keyword']],
        ], [], []);
        $index->prepare();
        $created = $index->index(['title' => 'First'], 0);
        $this->assertSame('0', $created['_id']);
        $this->assertSame('created', $created['result']);
        $updated = $index->index(['title' => 'Updated'], '0');
        $this->assertSame('0', $updated['_id']);
        $this->assertSame('updated', $updated['result']);

        $error = null;
        try {
            $index->index(['unknown_field' => 'Rejected'], 0);
        } catch (\Exception $e) {
            $error = $e;
        }
        $this->assertNotNull($error);
        $statusCode = $error instanceof \OpenSearch\Exception\HttpExceptionInterface
            ? $error->getStatusCode() : $error->getCode();
        $this->assertSame(400, $statusCode);
        $this->assertSame('Updated', $index->get(0)['_source']['title']);
        $this->assertSame(1, $this->countDocuments($index->getName()));
    }

}
