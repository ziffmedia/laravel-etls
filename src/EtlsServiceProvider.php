<?php

namespace ZiffMedia\LaravelEtls;

use Illuminate\Support\ServiceProvider;
use SplFileInfo;

class EtlsServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->discoverEtls();
    }

    protected function discoverEtls()
    {
        $etlsPath = app_path('Etls');

        $finder = new Finder();

        collect($finder->in($etlsPath)->files()->name('*.php'))->each(function (SplFileInfo $file) {
            $strippedBaseName = str_replace('.php', '', $file->getBasename());
            $className = 'App\Etls\\'.$strippedBaseName;

            $instance = new $className;

            // put these somewhere
        });
    }
}
