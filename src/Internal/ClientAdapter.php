<?php
namespace Wa72\ESTools\Internal;

use Elasticsearch\Client;
use Elastic\Elasticsearch\Client as ModernClient;
use OpenSearch\Client as OpenSearchClient;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use OpenSearch\Exception\NotFoundHttpException;

/** @internal Keeps the library's array/bool API across client generations. */
final class ClientAdapter
{
    public function __construct(private Client|ModernClient|OpenSearchClient $client)
    {
    }

    public function getClient(): Client|ModernClient|OpenSearchClient
    {
        return $this->client;
    }

    public function usesMappingTypes(): bool
    {
        return $this->client instanceof Client && (int) $this->client::VERSION < 7;
    }

    public function request(string $method, array $params = []): array|bool
    {
        if (!$this->usesMappingTypes()) {
            unset($params['type'], $params['parent'], $params['ttl']);
        }
        return $this->call($this->client, $method, $params);
    }

    public function indices(string $method, array $params = []): array|bool
    {
        return $this->call($this->client->indices(), $method, $params);
    }

    private function call(object $endpoint, string $method, array $params): array|bool
    {
        try {
            if ($endpoint instanceof OpenSearchClient && $method === 'index' && (string) ($params['id'] ?? '') === '0') {
                // opensearch-php 2.x treats the ID "0" as absent in its index endpoint.
                // Its raw API preserves that ID and the native response/error behavior.
                $uri = '/' . rawurlencode($params['index']) . '/_doc/0';
                $body = $params['body'];
                unset($params['index'], $params['id'], $params['body']);
                $response = $endpoint->request('PUT', $uri, ['params' => $params, 'body' => $body]);
            } else {
                $response = $endpoint->$method($params);
            }
        } catch (Missing404Exception|NotFoundHttpException $e) {
            throw new NotFoundException($e->getMessage(), 404, $e);
        } catch (ClientResponseException $e) {
            if ($e->getResponse()->getStatusCode() !== 404) {
                throw $e;
            }
            throw new NotFoundException($e->getMessage(), 404, $e);
        }
        if (is_array($response) || is_bool($response)) {
            return $response;
        }
        // HEAD responses have no JSON body, including a successful exists().
        return in_array($method, ['exists', 'existsAlias', 'ping'], true)
            ? $response->asBool()
            : $response->asArray();
    }
}
