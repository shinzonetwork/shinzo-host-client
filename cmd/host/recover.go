package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
)

const recoverFileFlag = "recover-file"

// recoverMnemonicFromFlag reads and validates --recover-file, if given,
// before the caller touches disk for anything else.
func recoverMnemonicFromFlag(cmd *cobra.Command) (string, error) {
	path, err := cmd.Flags().GetString(recoverFileFlag)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", nil
	}

	data, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied CLI flag, trusted the same as --config or --home.
	if err != nil {
		return "", fmt.Errorf("reading --%s: %w", recoverFileFlag, err)
	}

	mnemonic := strings.TrimSpace(string(data))
	if err := hostkey.ValidateMnemonic(mnemonic); err != nil {
		return "", fmt.Errorf("--%s: %w", recoverFileFlag, err)
	}

	return mnemonic, nil
}

// printGeneratedMnemonic writes mnemonic to w if hostkey.Ensure just
// generated a fresh one, it's never shown again after this.
func printGeneratedMnemonic(w io.Writer, mnemonic string) error {
	if mnemonic == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "\nGenerated a new node seed phrase. Write it down now, it will not be shown again:\n\n  %s\n\n", mnemonic)
	return err
}
