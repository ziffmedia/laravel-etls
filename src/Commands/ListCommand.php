<?php

namespace ZiffMedia\LaravelEtls\Commands;

use Illuminate\Support\Str;
use ZiffMedia\LaravelEtls\EtlExecutor;
use Illuminate\Console\Command;

class ListCommand extends Command
{
    protected $signature = 'etls:list';

    protected $description = 'List ETLs';

    public function handle()
    {
        $etls = config('etls.etl_classes');

        foreach ($etls as $etlName => $etlClass) {
            $this->output->writeln(Str::kebab($etlName) . " found in class $etlClass");
        }

        return 0;
    }
}
