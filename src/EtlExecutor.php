<?php

namespace ZiffMedia\LaravelEtls;

use Closure;
use Iterator;

class EtlExecutor
{
    protected array $runtimeInfo = [
        'extractor' => null,
        'loader' => null
    ];

    protected ?Closure $runtimeInfoCallback = null;
    protected int $runtimeInfoCallbackIterations = 10;

    public function withRuntimeInfoCallback(callable $runtimeInfoCallback, $afterIterations = 10): static
    {
        $this->runtimeInfoCallback = $runtimeInfoCallback;
        $this->runtimeInfoCallbackIterations = $afterIterations;

        return $this;
    }

    public function execute(AbstractEtl $etl, bool $incremental = false): array
    {
        $extractor = $etl->extractor();
        $loader = $etl->loader();

        $loader->prepare();

        $incrementalValue = $incremental ? $loader->getIncrementalLastValue() : false;

        $iteration = 0;

        foreach ($extractor->extract($incrementalValue) as $extractedData) {
            $this->runtimeInfo['extractor'] = $extractor->getRuntimeInfo();

            $transformedData = $etl->transform($extractedData);

            if ($transformedData instanceof Iterator) {
                foreach ($transformedData as $data) {
                    $loader->load($data);
                }
            } elseif ($transformedData !== null) {
                $loader->load($transformedData);
            }

            $this->runtimeInfo['loader'] = $loader->getRuntimeInfo();

            if (($iteration++ % $this->runtimeInfoCallbackIterations) === 0 && $this->runtimeInfoCallback !== null) {
                ($this->runtimeInfoCallback)($this->runtimeInfo, $iteration);
            }
        }

        $loader->cleanup();

        return $this->runtimeInfo;
    }

    public function getRuntimeInfo(): array
    {
        return $this->runtimeInfo;
    }
}
