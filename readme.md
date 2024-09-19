# Laravel ETLs

Define ETL pipelines to extract, transform, and load data from one source into your Laravel application.

# Installation
```php
composer require ziffmedia/laravel-etls
```

# Development
## Configuration
Upon installing this library, you'll first need to create a directory called `Etls` at `App/Etls`.
Next, create a class file in this directory that extends the `AbstractEtl` class, which implements
the required `extractor` and `loader` methods. The `extractor` method must return an object which
implements the `Extractor` interface, and the `loader` method must return an object which implements
the `Loader` interface. Here is an arbitrary example:

```php
<?php

namespace App\Etls;

use ZiffMedia\LaravelEtls\AbstractEtl;
use ZiffMedia\LaravelEtls\Contracts\Extractor;
use ZiffMedia\LaravelEtls\Contracts\Loader;
use ZiffMedia\LaravelEtls\DbExtractor;use ZiffMedia\LaravelEtls\DbLoader;

final class CustomersEtl extends AbstractEtl
{
    public function extractor(): Extractor
    {
        $extractor = new DbExtractor(app('db')->connection('external_db'));
        $extractor->query()
            ->select([
                'username'
                'email'
            ])
            ->from('users');

        return $extractor;
    }

    public function loader(): Loader
    {
        $loader = new DbLoader(DB::connection());
        $loader->table('customers')
            ->columns([
                'username',
                'email'
            ]);

        return $loader;
    }
}
```

The class above will register an ETL with the name `customers` that can be run using
Artisan. e.g., `artisan etls:run customers`. Please see the usage section below for additional details.

## Transformation
By default, the `AbstractEtl` class passes data from the `Extractor` to the `Loader` in the exact
"shape" it extracted it in. To modify data before loading it into your Laravel app, you can override
the `transform` method. Here's another arbitrary example:

```php
final class CustomersEtl extends AbstractEtl
{
    // ...other details from above

    public function transform(array $data): array|Iterator|null
    {
        $transformedData = array_merge($data, ['email_address' => $data['email']]);

        unset($transformedData['email']);

        return $transformedData;
    }
}
```

The above example would assume you're loading the `email` column from the data source into an
`email_address` field in the data destination. Naturally, real transformations will usually be more
involved.

## Key Mapping
By default, laravel-etls will look for an `id` field in both the data source and the destination
as the unique key to keep track of. Sometimes, those values are not the same type. For instance,
you might have an `id` of type `string` (e.g., a UUID) in the source destination, and an `id` of type
`int` in the destination. For cases like these, you'll want to ensure you have a legacy key set up
in your destination database so the library can know how to correctly map that data on subsequent
runs of the ETL. For this, you'll want to use the `uniqueColumns` method. Here's an arbitrary example:

```php
<?php

namespace App\Etls;

use ZiffMedia\LaravelEtls\AbstractEtl;
use ZiffMedia\LaravelEtls\Contracts\Extractor;
use ZiffMedia\LaravelEtls\Contracts\Loader;
use ZiffMedia\LaravelEtls\DbExtractor;use ZiffMedia\LaravelEtls\DbLoader;

final class CustomersEtl extends AbstractEtl
{
    public function extractor(): Extractor
    {
        $extractor = new DbExtractor(app('db')->connection('external_db'));
        $extractor->query()
            ->select([
                'id AS legacy_id',
                'username'
                'email'
            ])
            ->from('users');

        return $extractor;
    }

    public function loader(): Loader
    {
        $loader = new DbLoader(DB::connection());
        $loader->table('customers')
            ->columns([
                'username',
                'email'
            ])
            ->uniqueColumns(['legacy_id']);

        return $loader;
    }
}
```

# Usage
```shell
# Display the number of records from extractor and loader index for a given ETL.
artisan etls:info <name-of-your-etl>

# List the available ETLs to run.
artisan etls:list

# Run a specified ETL.
artisan etls:run <name-of-your-etl>
```
