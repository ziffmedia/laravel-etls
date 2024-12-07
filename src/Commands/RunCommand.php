<?php

namespace ZiffMedia\LaravelEtls\Commands;

use Illuminate\Support\Str;
use ZiffMedia\LaravelEtls\EtlExecutor;
use Illuminate\Console\Command;

class RunCommand extends Command
{
    protected $signature = 'etls:run {--incremental} {etl}';

    protected $description = 'Run ETLs';

    public function handle()
    {
        $etlName = $this->argument('etl');
        $isIncremental = $this->option('incremental');

        $etls = array_change_key_case(config('etls.etl_classes'), CASE_LOWER);

        $etlName = Str::of($etlName)->remove(['-', '_'])->lower()->toString();

        if (!isset($etls[$etlName])) {
            $this->output->error('That ETL was not found');

            return 1;
        }

        $etl = new $etls[$etlName];

        $etlExecutor = new EtlExecutor();

        $outputter = function ($runtimeInfo, $iteration = null) {
            $this->output->write("Extracted: {$runtimeInfo['extractor']['extracted_rows']} "
                . "Inserted: {$runtimeInfo['loader']['insert_count']} "
                . "Updated: {$runtimeInfo['loader']['update_count']} "
                . "Skipped: {$runtimeInfo['loader']['skip_count']} "
                . "Indexed: {$runtimeInfo['loader']['index_count']} "
                . "\r"
            );
        };

        if ($this->output->isVerbose()) {
            $etlExecutor->withRuntimeInfoCallback($outputter);
        }

        $etlExecutor->execute($etl, $isIncremental);

        if ($this->output->isVerbose()) {
            $this->output->writeln("\nComplete");
        }

        return 0;
    }
}
