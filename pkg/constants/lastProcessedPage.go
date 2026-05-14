package constants

// LastProcessedPage represents the last page number that was processed in a paginated query. This is used to track progress and resume processing from the correct point in case of interruptions.
type LastProcessedPage struct {
	Page int `json:"page"`
}
