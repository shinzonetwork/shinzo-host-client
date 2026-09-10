package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
)

const bannerTitle = "Node Identity"

func printKeyBanner(w io.Writer, keys host.NodeKeys) error {
	peerID, err := keys.PeerID()
	if err != nil {
		return fmt.Errorf("resolving peer id: %w", err)
	}
	shinzoAddr, err := keys.ShinzoAddress()
	if err != nil {
		return fmt.Errorf("resolving shinzo address: %w", err)
	}

	rows := [][2]string{
		{"Peer ID", peerID.String()},
		{"Operator (eth)", keys.OperatorAddress().Hex()},
		{"Operator (shinzo)", shinzoAddr},
		{"DID", keys.DID()},
	}

	labelWidth := 0
	for _, row := range rows {
		labelWidth = max(labelWidth, len(row[0]))
	}

	innerWidth := len(bannerTitle)
	for _, row := range rows {
		innerWidth = max(innerWidth, labelWidth+2+len(row[1]))
	}

	border := color.New(color.FgHiBlack).SprintFunc()
	title := color.New(color.FgCyan, color.Bold).SprintFunc()
	label := color.New(color.FgYellow).SprintFunc()
	value := color.New(color.FgWhite, color.Bold).SprintFunc()
	side := border("║")

	fmt.Fprintln(w, border("╔"+strings.Repeat("═", innerWidth+2)+"╗"))
	fmt.Fprintf(w, "%s %s %s\n", side, title(center(bannerTitle, innerWidth)), side)
	fmt.Fprintln(w, border("╠"+strings.Repeat("═", innerWidth+2)+"╣"))
	for _, row := range rows {
		plain := fmt.Sprintf("%-*s  %s", labelWidth, row[0], row[1])
		pad := strings.Repeat(" ", innerWidth-len(plain))
		fmt.Fprintf(w, "%s %s  %s%s %s\n", side, label(fmt.Sprintf("%-*s", labelWidth, row[0])), value(row[1]), pad, side)
	}
	fmt.Fprintln(w, border("╚"+strings.Repeat("═", innerWidth+2)+"╝"))

	return nil
}

func center(s string, width int) string {
	if len(s) >= width {
		return s
	}
	left := (width - len(s)) / 2
	right := width - len(s) - left
	return strings.Repeat(" ", left) + s + strings.Repeat(" ", right)
}
