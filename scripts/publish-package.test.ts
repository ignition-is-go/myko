import { expect, test } from "bun:test";
import { indexPath, isPublished } from "./publish-package";

function reply(body: string, status = 200): typeof fetch {
  return (async () => new Response(body, { status })) as typeof fetch;
}

test("crate index sharding", () => {
  expect(["a", "ab", "abc", "myko-server"].map(indexPath))
    .toEqual(["1/a", "2/ab", "3/a/abc", "my/ko/myko-server"]);
});
test("only an exact published version is skipped", async () => {
  const rows = '{"vers":"6.11.6"}\n{"vers":"6.12.0"}\n';
  expect(await isPublished("cargo", "myko", "6.12.0", reply(rows))).toBe(true);
  expect(await isPublished("cargo", "myko", "6.12.1", reply(rows))).toBe(false);
  expect(await isPublished("npm", "@myko/core", "6.12.0", reply('{"name":"@myko/core","version":"6.12.0"}'))).toBe(true);
  expect(await isPublished("npm", "@myko/core", "6.12.1", reply('{"name":"@myko/core","version":"6.12.0"}'))).toBe(false);
});
test("missing versions publish, registry failures abort", async () => {
  for (const ecosystem of ["cargo", "npm"]) {
    expect(await isPublished(ecosystem, "myko", "6.12.0", reply("", 404))).toBe(false);
    await expect(isPublished(ecosystem, "myko", "6.12.0", reply("", 503))).rejects.toThrow("HTTP 503");
    await expect(isPublished(ecosystem, "myko", "6.12.0", reply("invalid"))).rejects.toThrow();
  }
});
test("publish plan orders internal dev dependencies before their consumers", () => {
  const plan = Bun.spawnSync(["cargo", "flux", "plan", "publish", "--ordered"]);
  expect(plan.exitCode).toBe(0);
  const order = plan.stdout.toString();
  const metadata = Bun.spawnSync(["cargo", "metadata", "--no-deps", "--format-version", "1"]);
  expect(metadata.exitCode).toBe(0);
  const packages = JSON.parse(metadata.stdout.toString()).packages;
  for (const pkg of packages) {
    const current = order.indexOf(`${pkg.name}:publish [cargo]`);
    if (current < 0) continue;
    for (const dep of pkg.dependencies) {
      if (!dep.path) continue;
      const prerequisite = order.indexOf(`${dep.name}:publish [cargo]`);
      if (prerequisite < 0 || dep.name === pkg.name) continue;
      expect(prerequisite, `${dep.name} must publish before ${pkg.name}`).toBeLessThan(current);
    }
  }
});
