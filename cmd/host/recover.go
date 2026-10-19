package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
)

const recoverFileFlag = "recover-file"

// recoverMnemonicFromFlag reads and validates the mnemonic named by
// --recover-file, if the flag was given. Validating here, before the
// caller touches disk for anything else, means a bad file leaves
// nothing behind to clean up.
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
	if err := keys.ValidateMnemonic(mnemonic); err != nil {
		return "", fmt.Errorf("--%s: %w", recoverFileFlag, err)
	}

	return mnemonic, nil
}

// printGeneratedMnemonic writes mnemonic to w, if EnsureKeys just
// generated a fresh one. It's shown exactly this once and never saved
// anywhere, so the operator needs the chance to write it down now.
func printGeneratedMnemonic(w io.Writer, mnemonic string) error {
	if mnemonic == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "\nGenerated a new node seed phrase. Write it down now, it will not be shown again:\n\n  %s\n\n", mnemonic)
	return err
}
