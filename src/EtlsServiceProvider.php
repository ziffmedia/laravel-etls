<?php

namespace ZiffMedia\LaravelEtls;

use Illuminate\Support\ServiceProvider;
use SplFileInfo;
use Symfony\Component\Finder\Finder;
use ZiffMedia\LaravelEtls\Commands\RunCommand;

class EtlsServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        if ($this->app->runningInConsole()) {
            $this->discoverEtls();

            $this->commands([
                RunCommand::class
            ]);
        }
    }

    protected function discoverEtls()
    {
        $etlsPath = app_path('Etls');

        $finder = new Finder();

        collect($finder->in($etlsPath)->files()->name('*.php'))->each(function (SplFileInfo $file) {
            $strippedBaseName = str_replace('.php', '', $file->getBasename());
            $className = 'App\Etls\\'.$strippedBaseName;

            $instance = new $className;

            // @todo put these somewhere
        });
    }
}
