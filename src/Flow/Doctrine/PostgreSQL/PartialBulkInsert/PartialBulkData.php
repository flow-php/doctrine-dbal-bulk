<?php

declare(strict_types=1);

namespace Flow\Doctrine\PostgreSQL\PartialBulkInsert;

use Flow\Doctrine\Keys;

final class PartialBulkData
{
    private Keys $keys;

    private array $rows;

    private Keys $onConflictUpdateKeys;

    public function __construct(Keys $onConflictUpdateKeys, array $data)
    {
        $this->keys = new Keys(...\array_keys(\reset($data)));
        $this->rows = \array_map(
            function (int $index, array $row) {
                return $this->keys->indexedBy($index)->combineWith($row);
            },
            \range(0, \count($data) - 1),
            $data
        );
        $this->onConflictUpdateKeys = $onConflictUpdateKeys;
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
        return $this->onConflictUpdateKeys->asSQLSetUsingExcludedTable();
    }
}
