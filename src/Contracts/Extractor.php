<?php

namespace ZiffMedia\LaravelEtls\Contracts;

use Generator;

interface Extractor
{
    public function extract($incremental = false): Generator;
}
