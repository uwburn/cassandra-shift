import { readFile } from "fs/promises";

export default class BaseMigration {

  constructor(type, opts) {
    this.type = type;
    this.opts = opts;
  }

  get version() {
    return this.opts.version;
  }

  get name() {
    return this.opts.name;
  }

  static async loadFile(path) {
    return await readFile(path, {
      encoding: "utf8"
    });
  }

}


