<?php

declare(strict_types=1);

namespace Flow\Doctrine\Dbal;

final class Keys
{
    /**
     * @var string[]
     */
    private array $keys;

    public function __construct(string ...$keys)
    {
        $this->keys = $keys;
    }

    public function indexedBy(int $index) : self
    {
        return new self(
            ...$this->map(
                function (string $key) use ($index) {
                    return \sprintf('%s_%d', $key, $index);
                },
            )
        );
    }

    public function placeholders() : self
    {
        return new self(
            ...$this->map(
                function (string $key) {
                    return \sprintf(':%s', $key);
                }
            )
        );
    }

    public function combineWith(array $values) : array
    {
        return \array_combine($this->keys, $values);
    }

    public function commaSeparated() : string
    {
        return \implode(',', $this->keys);
    }

    public function asSQLValuesEntry() : string
    {
        return \sprintf('(%s)', $this->commaSeparated());
    }

    public function asSQLSetUsingExcludedTable() : string
    {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return \implode(
            ', ',
            $this->map(
                function (string $key) {
                    return \sprintf('%1$s = excluded.%1$s', $key);
                }
            )
        );
    }

    private function map(callable $callback) : array
    {
        return \array_map($callback, $this->keys);
    }
}
