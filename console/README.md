# Console

The node console is a Vite + React UI served at `/console`.

Source lives in the sibling repo `node-console` (same parent directory as this repo). Do not edit files under `console/dist` by hand.

## Refresh the embedded assets

```bash
make deps-console
```

Or:

```bash
go generate ./console
```

That builds `../node-console` (if present) and copies `dist/` here. To use a GitHub release instead:

```bash
CONSOLE_DIST_URL=https://github.com/<org>/node-console/releases/download/v0.1.0/dist.tar.gz go generate ./console
```

The host serves the SPA at `/console/` and keeps JSON APIs at `/api/*`, `/health`, and `/metrics`.
