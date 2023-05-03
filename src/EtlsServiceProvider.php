<?php

namespace ZiffMedia\LaravelEtls;

use Illuminate\Support\ServiceProvider;
use Illuminate\Support\Str;
use SplFileInfo;
use Symfony\Component\Finder\Finder;
use ZiffMedia\LaravelEtls\Commands\InfoCommand;
use ZiffMedia\LaravelEtls\Commands\ListCommand;
use ZiffMedia\LaravelEtls\Commands\RunCommand;

class EtlsServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        if ($this->app->runningInConsole()) {
            $this->discoverEtls();

            $this->commands([
                InfoCommand::class,
                ListCommand::class,
                RunCommand::class
            ]);
        }
    }

    protected function discoverEtls()
    {
        $etlsPath = app_path('Etls');

        $finder = new Finder();

        $collection = collect($finder->in($etlsPath)->files()->name('*.php'))->mapWithKeys(function (SplFileInfo $file) {
            $strippedBaseName = str_replace('.php', '', $file->getBasename());
            $className = 'App\Etls\\'.$strippedBaseName;

            if (Str::endsWith($strippedBaseName, 'Etl')) {
                $strippedBaseName = Str::substr($strippedBaseName, 0, -3);
            }

            return [$strippedBaseName => $className];
        });

        config()->set('etls.etl_classes', $collection->toArray());
    }
}
