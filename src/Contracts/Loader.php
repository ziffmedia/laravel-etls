<?php

namespace ZiffMedia\LaravelEtls\Contracts;

interface Loader
{
    public function load(array $data): void;
    public function prepare(): void;
    public function cleanup(): void;
    public function getRuntimeInfo(): array;
}
