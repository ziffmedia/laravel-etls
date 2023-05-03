<?php

namespace ZiffMedia\LaravelEtls;

use Generator;
use Illuminate\Database\Connection;
use Illuminate\Database\MySqlConnection;
use Illuminate\Database\PostgresConnection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\SqlServerConnection;
use RuntimeException;
use ZiffMedia\LaravelEtls\Contracts\Extractor;

class DbExtractor implements Extractor
{
    protected Connection $connection;

    protected Builder $query;

    protected array $uniqueColumns = ['id'];

    protected int $chunkSize = 1000;

    protected string $updateColumn = 'updated_at';

    protected array $runtimeInfo = [
        'extracted_rows' => 0,
        'current_query' => ''
    ];

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->query = $this->connection->query();
    }

    public function query(): Builder
    {
        return $this->query;
    }

    public function uniqueColumns(array $uniqueColumns)
    {
        $this->uniqueColumns = $uniqueColumns;
    }

    public function getUniqueColumns(): array
    {
        return $this->uniqueColumns;
    }

    public function updateColumn($updateColumn): static
    {
        $this->updateColumn = $updateColumn;

        return $this;
    }

    public function getChunkSize(): int
    {
        return $this->chunkSize;
    }

    public function getRuntimeInfo(): array
    {
        return $this->runtimeInfo;
    }

    public function extract(false|string $incremental = false): Generator
    {
        $hashedQuery = $this->createHashedQuery();
        $lastId = null;

        if ($incremental) {
            $hashedQuery->where($this->updateColumn, '>=', $incremental);
        }

        do {
            $clone = clone $hashedQuery;
            $pageQuery = $clone->forPageAfterId($this->chunkSize, $lastId, '_hash');
            $results = $pageQuery->get();
            $countResults = $results->count();

            $this->runtimeInfo['current_query'] = $pageQuery->toSql();
            $this->runtimeInfo['extracted_rows'] += $countResults;

            if ($countResults == 0) {
                break;
            }

            foreach ($results as $result) {
                yield (array) $result;
            }

            $lastId = $results->last()->_hash;

            unset($results);
        } while ($countResults == 1000);
    }

    protected function createHashedQuery(): Builder
    {
        $query = clone $this->query;

        if ($this->connection instanceof MySqlConnection) {
            $query->addSelect(
                $this->connection->raw(
                    'MD5(CONCAT_WS("|", '
                    . collect($this->getUniqueColumns())->implode(', ')
                    . ')) as `_hash`'
                )
            );
        } elseif ($this->connection instanceof SqlServerConnection) {
            $query->addSelect(
                $this->connection->raw(
                    "LOWER(CONVERT(varchar(32), HASHBYTES('md5', "
                    . collect($this->getUniqueColumns())
                        ->map(function ($uniqueColumn) {
                            return 'CAST(' . $this->connection->getQueryGrammar()->wrap($uniqueColumn) . ' as varchar)';
                        })
                        ->implode(" + '|' + ")
                    . '), 2)) as [_hash]'
                )
            );
        } elseif ($this->connection instanceof PostgresConnection) {
            $query->addSelect(
                $this->connection->raw(
                    "MD5(CONCAT_WS('|', "
                    . collect($this->getUniqueColumns())->implode(',')
                    . ')) AS "_hash"'
                )
            );
        } else {
            throw new RuntimeException('Currently only MySQL, Postgres, and SqlServer are supported inside the ' . __CLASS__);
        }

        $hashedQuery = $this->connection->query();
        $hashedQuery->fromSub($query, 'source');

        return $hashedQuery;
    }

    public function modifyQueryForIncremental(Builder $query)
    {

    }
}
