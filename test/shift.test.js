import { join } from "path";
import { Client } from "cassandra-driver";
import CassandraShift from "../lib/index.js";

const KEYSPACE = "shift_test";
const MIGRATION_TABLE = "schema_migrations";

let client;
let shift;

beforeAll(async () => {
  client = new Client({
    contactPoints: ["127.0.0.1"],
    localDataCenter: "datacenter1",
  });

  await client.connect();

  shift = new CassandraShift(
    [client],
    {
      keyspace: KEYSPACE,
      migrationTable: MIGRATION_TABLE,
      ensureKeyspace: true,
      useKeyspace: true,
      dir: join(import.meta.dirname,"test-migrations"),
    }
  );
});

afterAll(async () => {
  await client.shutdown();
});

describe("cassandra-shift integration tests", () => {

  test("clean() drops and recreates keyspace", async () => {
    await shift.clean();

    const res = await client.execute(
      "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
      [KEYSPACE],
      { prepare: true }
    );

    expect(res.rowLength).toBe(1);
  });

  test("migrate() applies all migrations", async () => {
    await shift.migrate();

    const tables = await client.execute(
      "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
      [KEYSPACE],
      { prepare: true }
    );

    const tableNames = tables.rows.map(r => r.table_name);
    expect(tableNames).toContain("users");
  });

  test("data migration executed correctly", async () => {
    const res = await client.execute("SELECT * FROM users");

    expect(res.rowLength).toBeGreaterThan(0);
    expect(res.rows[0]).toHaveProperty("name", "Alice");
  });

  test("migration table contains applied migrations", async () => {
    const res = await client.execute(
      `SELECT * FROM ${MIGRATION_TABLE}`
    );

    expect(res.rowLength).toBe(3);
  });

  test("validate() returns true for consistent migrations", async () => {
    const isValid = await shift.validate();
    expect(isValid).toBe(true);
  });

  test("validate() detects changed migration", async () => {
    await client.execute(
      `UPDATE ${MIGRATION_TABLE} SET checksum = 'invalid' WHERE version = 1`
    );

    const isValid = await shift.validate();
    expect(isValid).toBe(false);
  });

  test("validate(true) throws on inconsistency", async () => {
    await client.execute(
      `UPDATE ${MIGRATION_TABLE} SET checksum = 'invalid' WHERE version = 1`
    );

    await expect(shift.validate(true)).rejects.toThrow();
  });

});
