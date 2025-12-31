package attestation

type Transaction struct {
	Hash                 string            `json:"hash"`
	BlockHash            string            `json:"blockHash"`
	BlockNumber          int               `json:"blockNumber"`
	From                 string            `json:"from"`
	To                   string            `json:"to"`
	Value                string            `json:"value"`
	Gas                  string            `json:"gas"`
	GasPrice             string            `json:"gasPrice"`
	MaxFeePerGas         string            `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string            `json:"maxPriorityFeePerGas"`
	Input                string            `json:"input"`
	Nonce                string            `json:"nonce"`
	TransactionIndex     int               `json:"transactionIndex"`
	Type                 string            `json:"type"`
	ChainId              string            `json:"chainId"`
	V                    string            `json:"v"`
	R                    string            `json:"r"`
	S                    string            `json:"s"`
	Status               bool              `json:"status"`
	CumulativeGasUsed    string            `json:"cumulativeGasUsed"`
	EffectiveGasPrice    string            `json:"effectiveGasPrice"`
	Block                Block             `json:"block"`
	Logs                 []Log             `json:"logs"`
	AccessList           []AccessListEntry `json:"accessList"`

	Version []Version `json:"_version"`
	DocId   string    `json:"_docID"`
}
