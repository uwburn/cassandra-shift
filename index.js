"use strict";

const debug = require("debug")("cassandra-migration");

const fs = require("fs");
const path = require("path");
const EventEmitter = require("events");

const JsMigration = require("./JsMigration");
const CqlMigration = require("./CqlMigration");

async function ensureKeyspace(cassandraClient, keyspace) {
  debug(`Ensuring keyspace ${keyspace}, if not exists it will be created with default settings`);

  await cassandraClient.execute(`CREATE KEYSPACE IF NOT EXISTS ${keyspace}
  WITH replication = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
  }`);
}

async function useKeyspace(cassandraClient, keyspace) {
  debug(`Using keyspace ${keyspace}`);

  await cassandraClient.execute(`USE ${keyspace}`);
}

async function ensureMigrationTable(cassandraClient) {
  debug("Ensuring table migration_history exists");

  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS migration_history (
    version int,
    name text,
    type text,
    checksum text,
    installed_on timestamp,
    execution_time int,
    success boolean,
    PRIMARY KEY (version)
  )`);
}

async function getAppliedMigrations(cassandraClient) {
  debug("Retrieving table applied migration list");

  return (await cassandraClient.execute("SELECT * FROM migration_history")).rows;
}

async function loadAvailableMigration(dir, filename) {
  let match = filename.match(/^([0-9]+)__([A-z0-9_]*)\.(js|cql)$/);
  if (match.index !== 0)
    return;

  let type = match[3];
    
  let opts = {
    path: dir + path.sep + filename,
    version: parseInt(match[1]),
    name: match[2]
  };

  debug(`Loading migration definition ${filename}`);

  let migration;
  switch(type) {
  case "js":
    migration = new JsMigration(opts);
    break;
  case "cql":
    migration = new CqlMigration(opts);
    break;
  }

  await migration.load();

  return migration;
}

async function loadAvailableMigrations(dir) {
  debug("Loading available migrations");

  let migrationFiles = fs.readdirSync(dir);

  let migrations = [];
  for (let file of migrationFiles) {
    let m = await loadAvailableMigration(dir, file);
    if (m)
      migrations.push(m);
  }

  return migrations;
}

function checkAppliedMigrations(appliedMigrations, migrations) {
  debug("Checking applied migrations");

  if (appliedMigrations.length === 0)
    return;

  let lm = appliedMigrations[appliedMigrations.length - 1];
  if (!lm.success)
    throw new Error(`Migration ${lm.version} - "${lm.name}" failed, fix manually before retrying`);

  for (let i = 0; i < appliedMigrations.length; ++i) {
    let am = appliedMigrations[i];
    let m = migrations[i];

    if (am.version !== m.version)
      throw new Error(`Migration version mismatch: applied ${am.version}, defined ${m.version}`);

    if (am.name !== m.name)
      throw new Error(`Migration name mismatch: applied ${am.name}, defined ${m.name}`);

    if (am.checksum !== m.checksum)
      throw new Error(`Migration checksum mismatch: applied ${am.checksum}, defined ${m.checksum}`);
  }
}

async function executeMigration(cassandraClient, migration) {
  debug(`Applying migration ${migration.version} "${migration.name}"`);

  let now = new Date();

  let cachedError;
  let success;
  try {
    await migration.execute(cassandraClient);
    success = true;
  }
  catch(err) {
    cachedError = err;
    success = false;
  }

  await cassandraClient.execute(`INSERT INTO migration_history (version, name, type, checksum, installed_on, execution_time, success) VALUES (
    ?, ?, ?, ?, ?, ?, ?
  )`, [
    migration.version,
    migration.name,
    migration.type,
    migration.checksum,
    now,
    new Date() - now,
    success
  ], {
    prepare: true
  });

  if (!success)
    throw cachedError;
}

module.exports = class Shift extends EventEmitter {

  constructor(cassandraClients, opts) {
    super();
    
    this.cassandraClients = cassandraClients;
    this.opts = opts;
  }

  async migrate() {
    if (this.opts.ensureKeyspace) {
      await ensureKeyspace(this.cassandraClients[0], this.opts.keyspace);
      this.emit("ensuredKeyspace");
    }

    if (this.opts.useKeyspace) {
      await useKeyspace(this.cassandraClients[0], this.opts.keyspace);
      this.emit("usedKeyspace");
    }

    await ensureMigrationTable(this.cassandraClients[0]);
    this.emit("ensuredMigrationTable");

    let appliedMigrations = await getAppliedMigrations(this.cassandraClients[0]);
    let availableMigrations = await loadAvailableMigrations(this.opts.dir);

    checkAppliedMigrations(appliedMigrations, availableMigrations);
    this.emit("checkedAppliedMigrations");

    for (let i = appliedMigrations.length; i < availableMigrations.length; ++i) {
      let m = availableMigrations[i];
      await executeMigration(this.cassandraClients, m);
      this.emit("appliedMigration", {
        version: m.version,
        name: m.name,
        type: m.type
      });
    }

    debug("Migrations applied succeffully");
  }

  async clean() {

  }

  async info() {

  }

  async validate() {

  }
};