<?php
namespace Wa72\ESTools\Internal;

use Elasticsearch\Client;
use Elastic\Elasticsearch\Client as ModernClient;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Elasticsearch\Common\Exceptions\Missing404Exception;

/** @internal Keeps the library's array/bool API across client generations. */
final class ClientAdapter
{
    public function __construct(private Client|ModernClient $client)
    {
    }

    public function getClient(): Client|ModernClient
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
            $response = $endpoint->$method($params);
        } catch (Missing404Exception $e) {
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
