package attestation

import "github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"

type Block struct {
	Hash             string        `json:"hash"`
	Number           uint64        `json:"number"`
	Timestamp        string        `json:"timestamp"`
	ParentHash       string        `json:"parentHash"`
	Difficulty       string        `json:"difficulty"`
	TotalDifficulty  string        `json:"totalDifficulty"`
	GasUsed          string        `json:"gasUsed"`
	GasLimit         string        `json:"gasLimit"`
	BaseFeePerGas    string        `json:"baseFeePerGas"`
	Nonce            string        `json:"nonce"`
	Miner            string        `json:"miner"`
	Size             string        `json:"size"`
	StateRoot        string        `json:"stateRoot"`
	Sha3Uncles       string        `json:"sha3Uncles"`
	TransactionsRoot string        `json:"transactionsRoot"`
	ReceiptsRoot     string        `json:"receiptsRoot"`
	LogsBloom        string        `json:"logsBloom"`
	ExtraData        string        `json:"extraData"`
	MixHash          string        `json:"mixHash"`
	Uncles           []string      `json:"uncles"`
	Transactions     []Transaction `json:"transactions"`

	Version []attestation.Version `json:"_version"`
	DocId   string                `json:"_docID"`
}
