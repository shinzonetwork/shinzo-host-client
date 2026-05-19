<!--
  This README covers local setup, Docker, and deployment only.
  Do not add: architecture explanations, API reference, configuration 
  deep-dives, or troubleshooting guides. Those belong in the Shinzo 
  documentation site. If you're tempted to add a section, link to the docs 
  instead.
-->

# shinzo-host-client

![Build Status](https://img.shields.io/github/actions/workflow/status/shinzonetwork/shinzo-host-client/go-test.yml)
![License](https://img.shields.io/github/license/shinzonetwork/shinzo-host-client)
![Docker](https://img.shields.io/docker/v/shinzonetwork/shinzo-host-client)

Receives primitive blockchain data from Shinzo Indexers, applies Lens WASM transforms, and serves the resulting Views to subscriber nodes via an embedded DefraDB instance.

## Getting started

```shell
docker compose up
```

You will need a `~/config.yaml` on the host machine before starting. At minimum, set your keyring secret:

```shell
export DEFRA_KEYRING_SECRET=<your-secret>
```

> [!TIP]
> See [BUILD.md](./BUILD.md) for full build-from-source instructions.

## Configuration

The application reads from `config/config.yaml`. The only field you must set is `defradb.keyring_secret` (or the `DEFRA_KEYRING_SECRET` environment variable). All other fields have sensible defaults.

See the [Shinzo documentation site](https://docs.shinzo.network) for the full configuration reference.

## Deployment

See the [Shinzo documentation site](https://docs.shinzo.network) for production deployment instructions, hardware requirements, and network topology guidance.

## Contributing

Open an issue before submitting a PR. See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

[MIT](./LICENSE)
