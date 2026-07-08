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

A Host node for the Shinzo network. It pulls primitive blockchain data from Indexers, runs Lens WASM transforms, and serves the resulting Views to subscriber nodes via an embedded DefraDB instance.

## Getting started

Make sure `~/config.yaml` exists on the host machine, then:

```shell
docker compose up
```

Set your keyring secret before starting:

```shell
export DEFRA_KEYRING_SECRET=<your-secret>
```

Further instructions, as well as hardware recommendations, can be found at [docs.shinzo.network](https://docs.shinzo.network/hosts/overview).

> [!TIP]
> See [BUILD.md](./BUILD.md) for full build-from-source instructions.

## Configuration

The app reads from `config/config.yaml`. The only field you must set is `defradb.keyring_secret` (or the `DEFRA_KEYRING_SECRET` environment variable). Everything else has working defaults.

See [docs.shinzo.network](https://docs.shinzo.network/hosts/overview) for the full configuration reference.

## Deployment

See the [Shinzo documentation site](https://docs.shinzo.network) for production deployment instructions, hardware requirements, and network topology guidance.

## Contributing

Open an issue before submitting a PR. See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

[MIT](./LICENSE)
