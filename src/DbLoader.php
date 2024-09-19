<?php

namespace ZiffMedia\LaravelEtls;

use Closure;
use Illuminate\Database\Connection;
use Illuminate\Database\MySqlConnection;
use PDOStatement;
use RuntimeException;
use ZiffMedia\LaravelEtls\Contracts\Loader;

class DbLoader implements Loader
{
    protected Connection $connection;

    protected string $table = '';

    protected array $columns;

    protected array $columnsForInsertOnly = [];

    protected array $columnsForUpdateOnly = [];

    protected array $uniqueColumns = ['id'];

    protected string $updateColumn = 'updated_at';

    protected array $rowHashIndex = [];

    protected ?PDOStatement $insertStatement = null;

    protected ?PDOStatement $updateStatement = null;

    protected bool $inTransaction = false;

    protected int $iteration = 0;

    protected bool $ignoreDuplicates = true;

    protected ?Closure $onInsertDataAppender = null;

    protected ?Closure $onUpdateDataAppender = null;

    protected bool $performInserts = true;

    protected bool $performUpdates = true;

    protected bool $performDeletes = true;

    protected array $runtimeInfo = [
        'insert_query' => '',
        'update_query' => '',
        'insert_count' => 0,
        'update_count' => 0,
        'delete_count' => 0,
        'skip_count' => 0,
        'index_count' => 0
    ];

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function table(string $table): static
    {
        $this->table = $table;

        return $this;
    }

    public function columns(array $columns, array $insertOnlyColumns = [], array $updateOnlyColumns = []): static
    {
        $this->columns = $columns;
        $this->columnsForInsertOnly = $insertOnlyColumns;
        $this->columnsForUpdateOnly = $updateOnlyColumns;

        return $this;
    }

    public function updateColumn($updateColumn): static
    {
        $this->updateColumn = $updateColumn;

        return $this;
    }

    public function uniqueColumns($uniqueColumns): static
    {
        $this->uniqueColumns = $uniqueColumns;

        return $this;
    }

    public function withoutInserts(): static
    {
        $this->performInserts = false;

        return $this;
    }

    public function withoutUpdates(): static
    {
        $this->performUpdates = false;

        return $this;
    }

    public function withoutDeletes(): static
    {
        $this->performDeletes = false;

        return $this;
    }

    public function getUniqueColumns(): array
    {
        return $this->uniqueColumns;
    }

    public function onInsertDataAppender(callable $onInsertDataAppender): static
    {
        $this->onInsertDataAppender = $onInsertDataAppender;

        return $this;
    }

    public function onUpdateDataAppender(callable $onUpdateDataAppender): static
    {
        $this->onUpdateDataAppender = $onUpdateDataAppender;

        return $this;
    }

    public function prepare(): void
    {
        $this->createIndex();
    }

    public function load(array $data): void
    {
        $this->iteration++;

        if ($this->iteration % 1000 === 1) {
            $this->connection->beginTransaction();
        } elseif ($this->iteration % 1000 === 0) {
            $this->connection->commit();
        }

        $hash = $data['_hash'] ?? null;
        unset($data['_hash']);

        if (isset($this->rowHashIndex[$hash])) {
            $this->performUpdates ? $this->updateRow($data) : $this->runtimeInfo['skip_count']++;
        } else {
            $this->performInserts ? $this->insertRow($data) : $this->runtimeInfo['skip_count']++;
        }

        $this->rowHashIndex[$hash] = true;
    }

    public function cleanup(): void
    {
        if ($this->connection->transactionLevel() > 0) {
            $this->connection->commit();
        }
    }

    public function getRuntimeInfo(): array
    {
        return $this->runtimeInfo;
    }

    protected function createIndex()
    {
        $grammar = $this->connection->getQueryGrammar();
        $table = $grammar->wrapTable($this->table);

        if ($this->connection instanceof MySqlConnection) {
            $sql = 'SELECT MD5(CONCAT_WS("|", ' . implode(', ', $this->getUniqueColumns()) . ")) as hash FROM {$table}";
        } else {
            throw new RuntimeException('Only MySQL currently supported as loader type');
        }

        foreach ($this->connection->select($sql) as $row) {
            $this->rowHashIndex[$row->hash] = false;
        }

        $this->runtimeInfo['index_count'] = count($this->rowHashIndex);
    }

    protected function insertRow(array $data)
    {
        if (!$this->insertStatement) {
            $this->prepareInsertStatement();
        }

        if (is_callable($this->onInsertDataAppender)) {
            $data = call_user_func($this->onInsertDataAppender, $data);

            if (!is_array($data)) {
                throw new RuntimeException('onUpdateDataAppender() should return an array of $data to insert');
            }
        }

        $this->insertStatement->execute($this->createBindingData($data, array_merge($this->columns, $this->columnsForInsertOnly)));

        $this->runtimeInfo['insert_count'] += $this->insertStatement->rowCount();
    }

    protected function updateRow(array $data)
    {
        if (!$this->updateStatement) {
            $this->prepareUpdateStatement();
        }

        if (is_callable($this->onUpdateDataAppender)) {
            $data = call_user_func($this->onUpdateDataAppender, $data);

            if (!is_array($data)) {
                throw new RuntimeException('onUpdateDataAppender() should return an array of $data to insert');
            }
        }

        $this->updateStatement->execute($this->createBindingData($data, array_merge($this->columns, $this->columnsForUpdateOnly)));
        $this->runtimeInfo['update_count'] += $this->updateStatement->rowCount();
    }

    protected function createBindingData($data, $columns)
    {
        $newData = [];

        foreach ($columns as $column) {
            $newData[':' . $column] = $data[$column] ?? null;
        }

        return $newData;
    }

    protected function prepareInsertStatement()
    {
        $columns = collect($this->columns);

        if ($this->columnsForInsertOnly) {
            $columns = $columns->merge($this->columnsForInsertOnly);
        }

        $grammar = $this->connection->getQueryGrammar();

        $table = $grammar->wrapTable($this->table);

        $sql = "INSERT INTO {$table} ("
            . $columns->map(function ($column) use ($grammar) { return $grammar->wrap($column); })->implode(', ')
            . ') VALUES ('
            . $columns->map(function ($column) { return ':' . $column; })->implode(', ')
            . ')';

        $this->runtimeInfo['insert_query'] = $sql;

        $this->insertStatement = $this->connection->getPdo()->prepare($sql);
    }

    protected function prepareUpdateStatement()
    {
        $uniqueColumns = $this->getUniqueColumns();
        $sqlSets = $sqlWheres = [];

        $grammar = $this->connection->getQueryGrammar();

        $columns = collect($this->columns);

        if ($this->columnsForUpdateOnly) {
            $columns = $columns->merge($this->columnsForUpdateOnly);
        }

        foreach ($columns as $column) {
            $partialSql = $grammar->wrap($column) . " = :{$column}";

            if (!in_array($column, $uniqueColumns)) {
                $sqlSets[] = $partialSql;
            } else {
                $sqlWheres[] = $partialSql;
            }
        }

        if (!$sqlSets || !$sqlWheres) {
            throw new RuntimeException('A proper update statement could not be produced, either set columns or where columns are missing.');
        }

        $sql = 'UPDATE ' . $grammar->wrapTable($this->table)
            . ' SET ' . implode(', ', $sqlSets)
            . ' WHERE ' . implode(' AND ', $sqlWheres);

        $this->runtimeInfo['update_query'] = $sql;

        $this->updateStatement = $this->connection->getPdo()->prepare($sql);
    }

    public function getIncrementalLastValue(): string
    {
        // $grammar = $this->connection->getQueryGrammar();

        return $this->connection->query()
            ->select($this->updateColumn)
            ->from($this->table)
            ->whereNotNull($this->updateColumn)
            ->orderByDesc($this->updateColumn)
            ->limit(1)
            ->value($this->updateColumn) ?? '';
    }
}
