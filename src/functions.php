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
 * Compute difference between two multidimensional associative arrays
 *
 * @param array $array1
 * @param array $array2
 * @return array Associative array:
 *                  Key "-" contains elements from $array1 that have no match in $array2,
 *                  Key "+" contains elements from $array2 that have no match in $array1,
 *                  empty array if both input arrays are equal
 */
function compare_assoc_arrays($array1, $array2)
{
    if (empty($array1) && empty($array2)) {
        return [];
    } elseif (empty($array1) && !empty($array2)) {
        return ['+' => $array2];
    } elseif (!empty($array1) && empty($array2)) {
        return ['-' => $array1];
    } else {
        $d = [];
        $d1 = array_diff_assoc_recursive($array1, $array2);
        if (!empty($d1)) {
            $d['-'] = $d1;
        }
        $d2 = array_diff_assoc_recursive($array2, $array1);
        if (!empty($d2)) {
            $d['+'] = $d2;
        }
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