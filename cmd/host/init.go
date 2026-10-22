package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
)

func initCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a new Shinzo Host instance",
		Long: `Initialize a new Shinzo Host instance.

--data-dir is never saved to config.toml. If you set it here, pass the
same --data-dir to start too, every time, or start falls back to the
default location instead, a different, empty directory.`,
		Example: `  host init
  host init --home /data/shinzo-host
  host init --data-dir mnt/folder
  host init --home /data/shinzo-host --data-dir mnt/folder
  host init --logger.level warn --http.addr :9090
  host init --recover-file /path/to/mnemonic.txt`,
		RunE: runInit,
	}

	registerCommonFlags(cmd)
	cmd.Flags().String("recover-file", "", "recover an existing node from a file containing its mnemonic seed phrase, instead of generating a new one")

	return cmd
}

func runInit(cmd *cobra.Command, _ []string) (err error) {
	home, err := cmd.Flags().GetString("home")
	if err != nil {
		return err
	}

	dataDir, err := cmd.Flags().GetString("data-dir")
	if err != nil {
		return err
	}

	recoverMnemonic, err := recoverMnemonicFromFlag(cmd)
	if err != nil {
		return err
	}

	overrides, err := getOverrides(cmd)
	if err != nil {
		return err
	}

	// a bad override is caught before anything below,
	// keys included, ever touches disk
	if err := hostconfig.ValidateOverrides(overrides); err != nil {
		return err
	}

	paths, err := hostconfig.Resolve(home, dataDir, "")
	if err != nil {
		return err
	}

	keysExisted := hostkey.Exist(paths.KeyDir)

	nodeKeys, mnemonic, err := hostkey.Ensure(paths.KeyDir, recoverMnemonic)
	if err != nil {
		if recoverMnemonic != "" && errors.Is(err, hostkey.ErrAlreadyHaveKeys) {
			return fmt.Errorf("--%s: %w", recoverFileFlag, err)
		}
		return err
	}

	out := cmd.OutOrStdout()

	if err := printGeneratedMnemonic(out, mnemonic); err != nil {
		return err
	}

	cfg, configExisted, err := hostconfig.Ensure(home, dataDir, overrides)
	if err != nil {
		return err
	}

	switch {
	case configExisted && keysExisted:
		if _, err := fmt.Fprintf(out, "Already initialized at %s\n", cfg.Home); err != nil {
			return err
		}
	case configExisted:
		if _, err := fmt.Fprintf(out, "Config already exists at %s, keys are now set up\n", cfg.ConfigPath); err != nil {
			return err
		}
	default:
		if _, err := fmt.Fprintf(out, "Initialized Shinzo Host at %s\n", cfg.Home); err != nil {
			return err
		}
	}

	// overrides only ever get baked into a freshly written config, an
	// existing one is never rewritten, so say so rather than printing
	// values below that look like they took effect but didn't
	if configExisted && len(overrides) > 0 {
		if _, err := fmt.Fprintf(out,
			"Note: config already exists at %s, override flags were ignored; edit it directly to change a saved instance.\n",
			cfg.ConfigPath); err != nil {
			return err
		}
	}

	// --data-dir isn't saved to config.toml, so remind the operator here,
	// at the one moment they're most likely to miss it, right after
	// setting it, before they ever run start without repeating it
	if cmd.Flags().Changed("data-dir") {
		if _, err := fmt.Fprintf(out,
			"Note: --data-dir is not saved; pass --data-dir %s to start too, or it'll use the default location instead.\n",
			cfg.DataDir); err != nil {
			return err
		}
	}

	return printConfig(out, cfg, nodeKeys)
}
