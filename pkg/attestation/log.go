package attestation

type Log struct {
	Address          string      `json:"address"`
	Topics           []string    `json:"topics"`
	Data             string      `json:"data"`
	TransactionHash  string      `json:"transactionHash"`
	BlockHash        string      `json:"blockHash"`
	BlockNumber      int         `json:"blockNumber"`
	TransactionIndex int         `json:"transactionIndex"`
	LogIndex         int         `json:"logIndex"`
	Removed          string      `json:"removed"`
	Block            Block       `json:"block"`
	Transaction      Transaction `json:"transaction"`

	Version []Version `json:"_version"`
	DocId   string    `json:"_docID"`
}
