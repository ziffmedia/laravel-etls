<?php

namespace ZiffMedia\LaravelEtls;

use Iterator;
use ZiffMedia\LaravelEtls\Contracts\Extractor;
use ZiffMedia\LaravelEtls\Contracts\Loader;

abstract class AbstractEtl
{
    abstract public function extractor(): Extractor;
    abstract public function loader(): Loader;

    public function transform(array $data): array|Iterator|null
    {
        return $data;
    }
}
