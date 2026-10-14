<?php
namespace Wa72\ESTools\tests;

use PHPUnit\Framework\TestCase;
use function Wa72\ESTools\array_diff_assoc_recursive;
use function Wa72\ESTools\compare_assoc_arrays;
use function Wa72\ESTools\normalizeDotPathNotation;

/**
 * Unit tests for the helper functions in src/functions.php (no Elasticsearch needed)
 */
class FunctionsTest extends TestCase
{
    public function testArrayDiffAssocRecursive()
    {
        $a1 = ['a' => 1, 'b' => ['c' => 2, 'd' => 3], 'e' => [1, 2]];

        $this->assertEquals([], array_diff_assoc_recursive($a1, $a1));
        // additional elements in array2 are ignored
        $this->assertEquals([], array_diff_assoc_recursive($a1, $a1 + ['x' => 'y']));
        $this->assertEquals([], array_diff_assoc_recursive([], $a1));

        $a2 = ['a' => 1, 'b' => ['c' => 5, 'd' => 3], 'e' => [1, 2]];
        $this->assertEquals(['b' => ['c' => 2]], array_diff_assoc_recursive($a1, $a2));

        // missing keys and type mismatches
        $this->assertEquals(['b' => ['c' => 2, 'd' => 3]], array_diff_assoc_recursive($a1, ['a' => 1, 'b' => 'scalar', 'e' => [1, 2]]));
        $this->assertEquals(['a' => 1], array_diff_assoc_recursive($a1, ['b' => ['c' => 2, 'd' => 3], 'e' => [1, 2]]));
        // null values are compared correctly
        $this->assertEquals([], array_diff_assoc_recursive(['n' => null], ['n' => null]));
        $this->assertEquals(['n' => null], array_diff_assoc_recursive(['n' => null], []));
    }

    public function testCompareAssocArraysEqual()
    {
        $this->assertEquals([], compare_assoc_arrays([], []));
        $this->assertEquals([], compare_assoc_arrays(['a' => ['b' => 1]], ['a' => ['b' => 1]]));
    }

    public function testCompareAssocArraysEmpty()
    {
        $this->assertEquals(['+' => ['a' => 1]], compare_assoc_arrays([], ['a' => 1]));
        $this->assertEquals(['-' => ['a' => 1]], compare_assoc_arrays(['a' => 1], []));
    }

    public function testCompareAssocArraysScalars()
    {
        $this->assertEquals(['-' => 'x', '+' => 'y'], compare_assoc_arrays('x', 'y'));
    }

    public function testCompareAssocArraysNested()
    {
        $a1 = [
            '_doc' => [
                'properties' => [
                    'title' => ['type' => 'text', 'analyzer' => 'my_analyzer'],
                    'only_in_1' => ['type' => 'keyword'],
                ]
            ]
        ];
        $a2 = [
            '_doc' => [
                'properties' => [
                    'title' => ['type' => 'text', 'analyzer' => 'other_analyzer'],
                    'only_in_2' => ['type' => 'integer'],
                ]
            ]
        ];
        $expected = [
            '-' => [
                '_doc.properties.title.analyzer' => 'my_analyzer',
                '_doc.properties.only_in_1' => ['type' => 'keyword'],
            ],
            '+' => [
                '_doc.properties.title.analyzer' => 'other_analyzer',
                '_doc.properties.only_in_2' => ['type' => 'integer'],
            ],
        ];
        $this->assertEquals($expected, compare_assoc_arrays($a1, $a2));
    }

    public function testNormalizeDotPathNotation()
    {
        $array = [
            'index.mapping.single_type' => true,
            'index' => ['number_of_shards' => 1],
            'plain' => 'value',
        ];
        normalizeDotPathNotation($array);
        $this->assertEquals([
            'index' => [
                'number_of_shards' => 1,
                'mapping' => ['single_type' => true],
            ],
            'plain' => 'value',
        ], $array);
    }
}
