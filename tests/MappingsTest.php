<?php
namespace Wa72\ESTools\tests;

use PHPUnit\Framework\TestCase;
use Wa72\ESTools\Internal\Mappings;

class MappingsTest extends TestCase
{
    public function testLegacyMappingIsUnwrappedOnlyForModernServers(): void
    {
        $body = ['properties' => ['title' => ['type' => 'text']]];
        $this->assertSame('article', Mappings::type(['article' => $body]));
        $this->assertSame($body, Mappings::normalize(['mappings' => ['article' => $body]], false));
        $this->assertSame(['article' => $body], Mappings::normalize(['article' => $body], true));
    }

    public function testTypelessMappingOptionsAreNotMistakenForTypes(): void
    {
        foreach (['properties' => ['title' => ['type' => 'text']], 'dynamic' => false,
                  '_source' => ['enabled' => false], '_meta' => ['version' => 1],
                  'runtime' => ['day' => ['type' => 'keyword']]] as $key => $value) {
            $body = [$key => $value];
            $this->assertSame('_doc', Mappings::type($body));
            $this->assertSame($body, Mappings::normalize($body, false));
            $this->assertSame(['_doc' => $body], Mappings::normalize(['mappings' => $body], true));
        }
        $this->assertSame([], Mappings::normalize([], false));
        $this->assertSame([], Mappings::normalize([], true));
    }

    public function testMultipleLegacyTypesAreRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Mappings::normalize(['one' => ['properties' => []], 'two' => ['properties' => []]], false);
    }
    public function testDefaultSourceConfigurationDoesNotCauseMappingDifferences(): void
    {
        $body = ['properties' => ['title' => ['type' => 'text']]];
        $this->assertSame($body, Mappings::forComparison($body + ['_source' => ['enabled' => true]], false));
        $this->assertSame(['_doc' => $body], Mappings::forComparison([
            '_doc' => $body + ['_source' => ['enabled' => true]],
        ], true));
        $disabled = $body + ['_source' => ['enabled' => false]];
        $this->assertSame($disabled, Mappings::forComparison($disabled, false));
        $filtered = $body + ['_source' => ['includes' => ['title']]];
        $this->assertSame($filtered, Mappings::forComparison([
            'properties' => $body['properties'], '_source' => ['enabled' => true, 'includes' => ['title']],
        ], false));
    }

}
