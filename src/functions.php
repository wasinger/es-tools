<?php
namespace Wa72\ESTools;

/**
 * Check that all keys from $array1 are present in $array2 and have the same value
 *
 * additional elements from $array2 are ignored
 *
 * @param array $array1
 * @param array $array2
 * @return array Array containing elements from array1 that have no match in array2
 */
function array_diff_assoc_recursive($array1, $array2)
{
    $d = [];
    foreach ($array1 as $key => $value)
    {
        if (is_array($value))
        {
            if(!isset($array2[$key]) || !is_array($array2[$key])) {
                $d[$key] = $value;
            } else {
                $new_diff = array_diff_assoc_recursive($value, $array2[$key]);
                if (!empty($new_diff))
                {
                    $d[$key] = $new_diff;
                }
            }
        }
        elseif (!\array_key_exists($key, $array2) || $array2[$key] != $value)
        {
            $d[$key] = $value;
        }
    }
    return $d;
}

/**
 * Compute difference between two multidimensional associative arrays.
 *
 * @param array $array1
 * @param array $array2
 * @return array Associative array:
 *                  Key "-" contains elements from $array1 that have no match in $array2,
 *                  Key "+" contains elements from $array2 that have no match in $array1,
 *                  element key paths are given in dot notation (a.b.c instead of [a][b][c]);
 *                  empty array if both input arrays are equal
 */
function compare_assoc_arrays($array1, $array2)
{
    if ($array1 == $array2) {
        return [];
    } elseif (empty($array1) && !empty($array2)) {
        return ['+' => $array2];
    } elseif (!empty($array1) && empty($array2)) {
        return ['-' => $array1];
    } elseif ((!is_array($array1) || !is_array($array2)) &&  $array1 != $array2) {
        return ['-' => $array1, '+' => $array2];
    } else {
        $d = [];

        $compare = function($a1, $a2, string $path, &$r) use (&$compare) {
            if ($a1 == $a2) {
                return;
            } elseif (empty($a1) && !empty($a2)) {
                $r['+'][$path] = $a2;
                return;
            } elseif (!empty($a1) && empty($a2)) {
                $r['-'][$path] = $a1;
                return;
            } elseif ((!is_array($a1) || !is_array($a2)) &&  $a1 != $a2) {
                $r['-'][$path] = $a1;
                $r['+'][$path] = $a2;
                return;
            } else {
                foreach ($a1 as $k1 => $v1) {
                    $subpath = $path . ($path ? '.' : '') . $k1;
                    if (isset($a2[$k1])) {
                        $compare($v1, $a2[$k1], $subpath, $r);
                    } else {
                        $r['-'][$subpath] = $v1;
                    }
                }
                foreach ($a2 as $k2 => $v2) {
                    $subpath = $path . ($path ? '.' : '') . $k2;
                    if (!isset($a1[$k2])) {
                        $r['+'][$subpath] = $v2;
                    }
                }
            }
        };
        $compare($array1, $array2, '', $d);

        return $d;
    }
}

/**
 * normalize dot path notation
 * $array['index.mapping.single_type'] -> $array['index']['mapping']['single_type']
 *
 * @param $array
 */
function normalizeDotPathNotation(&$array)
{
    foreach ($array as $key => $value) {
        if (strpos($key, '.')) {
            $segments = \explode('.', $key);
            $a = &$array;
            foreach ($segments as $segment) {
                $a = &$a[$segment];
            }
            $a = $value;
            unset($array[$key]);
        }
    }
}