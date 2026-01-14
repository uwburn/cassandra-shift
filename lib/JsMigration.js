import checksum from "checksum";
import BaseMigration from "./BaseMigration.js";

export default class JsMigration extends BaseMigration {

  constructor(opts) {
    super("JS", opts);
  }

  async load() {
    const { default: module } = await import(this.opts.path);
    this.fn = module;
    this.src = await BaseMigration.loadFile(this.opts.path);
    this.checksum = checksum(this.src);
  }

  async execute(cassandraClients) {
    return this.fn(cassandraClients);
  }

}