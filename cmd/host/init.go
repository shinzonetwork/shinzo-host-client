package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/logger"
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

	cfg, err := hostconfig.Setup(home, dataDir, overrides)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Initialized Shinzo Host at %s\n", cfg.Home); err != nil {
		return err
	}

	log, sync, err := logger.New(logger.Config{
		Level:       cfg.Logger.Level,
		Development: cfg.Logger.Development,
	})
	if err != nil {
		return err
	}
	defer func() {
		// Only surfaced if nothing else already failed: a flush error
		// shouldn't mask whatever real error the command hit first.
		if syncErr := sync(); syncErr != nil && err == nil {
			err = syncErr
		}
	}()

	nodeKeys, mnemonic, err := keys.EnsureKeys(cfg.KeyDir, recoverMnemonic, true, log.Sugar())
	if err != nil {
		if recoverMnemonic != "" && errors.Is(err, keys.ErrRecoverMismatch) {
			return fmt.Errorf("--%s: %w", recoverFileFlag, err)
		}
		return err
	}

	if err := printGeneratedMnemonic(cmd.OutOrStdout(), mnemonic); err != nil {
		return err
	}

	// --data-dir isn't saved to config.toml, so remind the operator here,
	// at the one moment they're most likely to miss it: right after
	// setting it, before they ever run start without repeating it.
	if cmd.Flags().Changed("data-dir") {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(),
			"Note: --data-dir is not saved; pass --data-dir %s to start too, or it'll use the default location instead.\n",
			cfg.DataDir); err != nil {
			return err
		}
	}

	return printConfig(cmd.OutOrStdout(), cfg, nodeKeys)
}
