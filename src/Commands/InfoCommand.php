<?php

namespace ZiffMedia\LaravelEtls\Commands;

use Illuminate\Support\Str;
use ZiffMedia\LaravelEtls\AbstractEtl;
use ZiffMedia\LaravelEtls\EtlExecutor;
use Illuminate\Console\Command;

class InfoCommand extends Command
{
    protected $signature = 'etls:info {etl}';

    protected $description = 'Run ETLs';

    public function handle()
    {
        $etlName = $this->argument('etl');

        $etls = array_change_key_case(config('etls.etl_classes'), CASE_LOWER);

        $etlName = Str::of($etlName)->remove(['-', '_'])->lower()->toString();

        if (!isset($etls[$etlName])) {
            $this->output->error('That ETL was not found');

            return 1;
        }

        $etlClass = $etls[$etlName];

        if (! class_exists($etlClass)) {
            $this->error("Could not load ETL class $etlClass");

            return 1;
        }

        /** @var AbstractEtl $etl */
        $etl = new $etlClass;

        $extractor = $etl->extractor();
        $extractorQuery = $extractor->query();
        $extractorQueryCount = $extractorQuery->count();

        $this->output->writeln("Extractor record count: $extractorQueryCount");

        $loader = $etl->loader();

        $loader->prepare();
        $loaderIndexedRecordCount = $loader->getRuntimeInfo()['index_count'];

        $this->output->writeln("Loader indexed record count: $loaderIndexedRecordCount");

        if ($this->output->isVerbose()) {
            $this->newLine();

            $this->output->writeln('Extractor query: ');
            $this->output->writeln($extractorQuery->toSql());
        }

        return 0;
    }
}
