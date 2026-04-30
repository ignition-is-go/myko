// Applies publishConfig overrides to package.json before npm publish.
// npm's publishConfig only supports registry/access/tag — this script
// handles exports, main, and types overrides for monorepo packages
// that export source TS locally but ship compiled dist to npm.

const pkgPath = `${process.cwd()}/package.json`;
const pkg = await Bun.file(pkgPath).json();

if (pkg.publishConfig) {
  Object.assign(pkg, pkg.publishConfig);
  delete pkg.publishConfig;
  await Bun.write(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
}
