package hostconfig

import (
	"bytes"
	"text/template"
)

const defaultConfigTemplate = `# Shinzo Host Config

# Local instance name. Sets the folder this host's data lives under,
# ~/.shinzo/host/<name>.
# js-escaped defensively even though validate already restricts Name's
# charset — cheap insurance if that charset ever loosens.
name = "{{ js .Name }}"

`

func render(cfg Config) ([]byte, error) {
	tmpl, err := template.New("config.toml").Parse(defaultConfigTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
