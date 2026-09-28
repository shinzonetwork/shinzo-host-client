package hostconfig

import "github.com/pelletier/go-toml/v2"

const configHeader = "# Shinzo Host Config\n\n"

func render(cfg Config) ([]byte, error) {
	data, err := toml.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	return append([]byte(configHeader), data...), nil
}
