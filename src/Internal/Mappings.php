<?php
namespace Wa72\ESTools\Internal;

/** @internal Accept both legacy single-type and modern typeless mappings. */
final class Mappings
{
    private const ROOT_KEYS = [
        'properties', 'dynamic', 'dynamic_templates', 'date_detection',
        'numeric_detection', 'dynamic_date_formats', '_source', '_meta',
        '_routing', '_field_names', '_size', 'enabled', 'runtime', 'subobjects',
    ];

    public static function type(array $mappings): string
    {
        $mappings = $mappings['mappings'] ?? $mappings;
        if (!$mappings || array_intersect(self::ROOT_KEYS, array_keys($mappings))) {
            return '_doc';
        }
        if (count($mappings) !== 1 || !is_array(reset($mappings))) {
            throw new \InvalidArgumentException('Mappings must be typeless or contain exactly one mapping type.');
        }
        return array_key_first($mappings);
    }

    public static function normalize(array $mappings, bool $typed): array
    {
        $mappings = $mappings['mappings'] ?? $mappings;
        $type = self::type($mappings);
        $body = $mappings[$type] ?? $mappings;
        if (!$body) {
            return [];
        }
        return $typed ? [$type => $body] : $body;
    }

    public static function forComparison(array $mappings, bool $typed): array
    {
        if ($typed) {
            foreach ($mappings as &$body) {
                $body = self::forComparison($body, false);
            }
            unset($body);
            return $mappings;
        }
        // Elasticsearch omits the default _source configuration in getMapping().
        if (($mappings['_source']['enabled'] ?? null) === true) {
            unset($mappings['_source']['enabled']);
            if (!$mappings['_source']) {
                unset($mappings['_source']);
            }
        }
        return $mappings;
    }
}
