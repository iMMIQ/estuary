import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const web = join(import.meta.dir, "..");
const temporary = await mkdtemp(join(tmpdir(), "estuary-contract-"));
const check = process.argv.includes("--check");
try {
  const generate = Bun.spawnSync(
    [
      "cargo",
      "run",
      "--locked",
      "--features",
      "config-contract",
      "--example",
      "config_contract",
      "--",
      temporary,
    ],
    { cwd: join(web, ".."), stdout: "inherit", stderr: "inherit" },
  );
  if (generate.exitCode !== 0) throw new Error("Rust contract generation failed");
  for (const name of (await readdir(temporary)).sort()) {
    const relative = `src/generated/${name}`;
    const source = await readFile(join(temporary, name), "utf8");
    const formatted = Bun.spawnSync(
      [join(web, "node_modules/.bin/biome"), "format", `--stdin-file-path=${relative}`],
      { cwd: web, stdin: new TextEncoder().encode(source), stderr: "inherit" },
    );
    if (formatted.exitCode !== 0) throw new Error(`Could not format ${relative}`);
    const content = new TextDecoder().decode(formatted.stdout);
    const destination = join(web, relative);
    if (check) {
      if ((await readFile(destination, "utf8")) !== content)
        throw new Error(`${relative} is outdated; run bun run contract:generate`);
    } else {
      await writeFile(destination, content);
    }
  }
  console.log(check ? "Configuration contract is current" : "Configuration contract generated");
} finally {
  await rm(temporary, { recursive: true, force: true });
}
