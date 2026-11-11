// NOTE(ts): A failed multi-package release must be resumable without republishing immutable versions.
export function indexPath(name: string): string {
  if (name.length === 1) return `1/${name}`;
  if (name.length === 2) return `2/${name}`;
  if (name.length === 3) return `3/${name[0]}/${name}`;
  return `${name.slice(0, 2)}/${name.slice(2, 4)}/${name}`;
}

export async function isPublished(
  ecosystem: string,
  name: string,
  version: string,
  request: typeof fetch = fetch,
): Promise<boolean> {
  const url = ecosystem === "cargo"
    ? `https://index.crates.io/${indexPath(name)}`
    : `https://registry.npmjs.org/${encodeURIComponent(name)}/${encodeURIComponent(version)}`;
  const response = await request(url, { signal: AbortSignal.timeout(30_000) });
  if (response.status === 404) return false;
  if (!response.ok) throw new Error(`Registry lookup failed for ${name}: HTTP ${response.status}`);
  if (ecosystem === "cargo") {
    return (await response.text()).trim().split("\n").filter(Boolean)
      .some((line) => JSON.parse(line).vers === version);
  }
  const entry = await response.json() as { name: string; version: string };
  return entry.name === name && entry.version === version;
}

if (import.meta.main) {
  const ecosystem = process.argv[2];
  if (ecosystem !== "cargo" && ecosystem !== "npm") throw new Error("Expected cargo or npm");
  const manifest = ecosystem === "cargo"
    ? Bun.TOML.parse(await Bun.file("Cargo.toml").text()).package
    : await Bun.file("package.json").json();
  const { name, version } = manifest as { name: string; version: string };
  if (typeof name !== "string" || typeof version !== "string") throw new Error("Missing package name/version");
  if (await isPublished(ecosystem, name, version)) {
    console.log(`Already published: ${name}@${version}`);
  } else {
    const command = ecosystem === "cargo"
      ? ["cargo", "publish", "--locked"]
      : ["npm", "publish", "--provenance=false", "--access", "public", "--tag", version.includes("-") ? "canary" : "latest"];
    const child = Bun.spawn(command, { stdin: "inherit", stdout: "inherit", stderr: "inherit" });
    process.exitCode = await child.exited;
  }
}
