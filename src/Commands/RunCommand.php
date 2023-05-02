<?php

namespace ZiffMedia\LaravelEtls\Commands;

use App\Etls\MerchantSeoDataEtl;
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

        $etlClass = 'App\Etls\\'.$etlName;

        if (! class_exists($etlClass)) {
            $this->error("Could not load ETL class $etlClass");

            return 1;
        }

        $etl = new $etlClass;

        // @todo get the etl

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
    }
}
