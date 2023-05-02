<?php

namespace ZiffMedia\LaravelEtls\Contracts;

use Generator;

interface Extractor
{
    public function extract(false|string $incrementalValue = false): Generator;
}
