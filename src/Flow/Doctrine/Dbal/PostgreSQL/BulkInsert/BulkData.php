<?php

declare(strict_types=1);

namespace Flow\Doctrine\Dbal\PostgreSQL\BulkInsert;

use Flow\Doctrine\Dbal\Keys;

final class BulkData
{
    private Keys $keys;

    private array $rows;

    public function __construct(array $data)
    {
        $this->keys = new Keys(...\array_keys(\reset($data)));
        $this->rows = \array_map(
            function (int $index, array $row) {
                return $this->keys->indexedBy($index)->combineWith($row);
            },
            \range(0, \count($data) - 1),
            $data
        );
    }

    public function placeholders() : string
    {
        return \implode(
            ',',
            \array_map(
                function (array $row) {
                    return (new Keys(...\array_keys($row)))->placeholders()->asSQLValuesEntry();
                },
                $this->rows
            )
        );
    }

    public function parameters() : array
    {
        return \array_merge(...$this->rows);
    }

    public function keys() : string
    {
        return $this->keys->commaSeparated();
    }

    public function onConflictUpdate() : string
    {
        return $this->keys->asSQLSetUsingExcludedTable();
    }
}
