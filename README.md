# cassandra-shift

`cassandra-shift` is a Node.js library for managing forward-only database migrations in Apache Cassandra.

It provides a structured way to define, apply, validate, and inspect migrations, following Cassandra best practices and avoiding rollback-based workflows.

## Overview

Schema evolution in Cassandra is inherently forward-only and does not lend itself well to traditional rollback mechanisms.
`cassandra-shift` embraces this approach by offering:

- Deterministic, ordered migrations
- Support for both CQL and JavaScript migrations
- Migration state tracking in a dedicated table
- Validation and inspection utilities
- Support for multiple Cassandra clients

## Features

- Forward-only migrations
- CQL and JavaScript-based migrations
- Multiple Cassandra clients support
- Automatic migration state tracking
- Migration validation and inspection
- Minimal and explicit API

## Installation

Install the package using npm:

`npm install cassandra-shift`

or yarn:

`yarn add cassandra-shift`

## Project Structure

A typical project using `cassandra-shift`:

```(text)
.
├── migrations/
│   ├── 001_create_users_table.cql
│   ├── 002_add_age_column.js
│   └── 003_update_posts_table.cql
└── migrate.js
```

Migration files are executed in lexicographical order.

## Writing Migrations

### CQL Migrations

CQL migrations are plain .cql files containing valid Cassandra Query Language statements.

Example:

`migrations/001_create_users_table.cql`

```(cql)
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  name text,
  email text,
  created_at timestamp
);
```

### JavaScript Migrations

JavaScript migrations must export a function.

Example:

`migrations/002_add_age_column.js`

```(javascript)
module.exports = async function (clients) {
  const writeClient = clients[0];

  await writeClient.execute(`
    ALTER TABLE users ADD age int;
  `);
};
```

The clients parameter is the same array of Cassandra clients passed to the migrator.

## Usage

### Creating the Migrator

The library exports a class.

The constructor accepts:

- An array of Cassandra clients
- An options object

```(javascript)
import { Client } from "cassandra-driver";
import CassandraShift from "cassandra-shift";

const readClient = new Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
});

const writeClient = new Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
});

await readClient.connect();
await writeClient.connect();

const shift = new CassandraShift(
  [readClient, writeClient],
  {
    keyspace: "my_keyspace",
    migrationTable: "schema_migrations",
    ensureKeyspace: true,
    useKeyspace: true,
    dir: "./migrations",
  }
);
```

### Options

The options object supports the following properties:

| Name | Type | Description |
|------|------|-------------|
| keyspace | string | Keyspace used for migrations |
| migrationTable	string	Table used to store migration state
| ensureKeyspace | boolean | Create the keyspace if it does not exist |
| useKeyspace | boolean | Execute `USE <keyspace>` if the client is not bound to a default keyspace |
| dir | string | Directory containing migration files |

### Methods

`migrate()`

Applies all pending migrations in order.

`await shift.migrate();`

Only migrations that have not yet been applied will be executed.

`clean()`

Drops the keyspace and recreates it.

`await shift.clean();`

This is a destructive operation and should only be used in development or testing environments.

`info()`

Returns information about the current migration state.

`const info = await shift.info();`

This typically includes applied migrations and pending ones.

`validate(rethrowError = false)`

Validates that the defined migrations match the migrations already applied in the database.

`const isValid = await shift.validate();`

If rethrowError is set to true, validation errors will be thrown instead of returning false:

`await shift.validate(true);`

## Migration Strategy

`cassandra-shift` does not support rollback operations.

Once a migration has been applied, it is considered permanent.
This design encourages:

- Additive and backward-compatible schema changes
- Immutable migration files
- Thorough testing before production deployment

## Migration Integrity

`cassandra-shift` stores a **hash of each applied migration** in the migration table.

When migrations are executed, their content is hashed and persisted together with the migration identifier.  
On subsequent runs, the library compares the stored hash with the hash of the migration file currently on disk.

If a migration file has been modified after being applied, the hash mismatch is detected and the migration state is considered **inconsistent**.

This mechanism ensures:

- Applied migrations are immutable
- Accidental or manual changes to migration files are detected
- Safer schema evolution in distributed environments

Hash validation is performed automatically during migration execution and can also be explicitly checked using the `validate()` method.

## Best Practices

- Never modify migrations that have already been applied
- Keep migrations small and focused
- Prefer additive schema changes
- Use validate() in CI pipelines or when apps relying on migrations are started
- Use clean() only in non-production environments

## Contributing

Contributions are welcome.

- Fork the repository
- Create a new branch
- Commit your changes
- Ensure coding style is respected
- Open a pull request

## License

MIT