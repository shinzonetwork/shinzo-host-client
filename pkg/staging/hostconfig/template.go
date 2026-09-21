package hostconfig

import (
	"bytes"
	"text/template"
)

// DefaultConfigTemplate is the config.toml template.
const DefaultConfigTemplate = `# Shinzo Host Config

# Local instance name. Sets the folder this host's data lives under,
# ~/.shinzo/host/<name>.
name = "{{.Name}}"

`

// Render fills the config template with cfg and returns the result.
func Render(cfg Config) ([]byte, error) {
	tmpl, err := template.New("config.toml").Parse(DefaultConfigTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
