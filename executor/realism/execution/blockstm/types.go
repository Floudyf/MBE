package blockstm

import "fmt"

type TxnIndex uint32
type Incarnation uint32

type Version struct {
	Txn         TxnIndex    `json:"txn_index"`
	Incarnation Incarnation `json:"incarnation"`
}

func (v Version) String() string {
	return fmt.Sprintf("%d:%d", v.Txn, v.Incarnation)
}

func (v Version) Less(other Version) bool {
	if v.Txn != other.Txn {
		return v.Txn < other.Txn
	}
	return v.Incarnation < other.Incarnation
}

type ReadDescriptor struct {
	Key          string   `json:"key"`
	FromBase     bool     `json:"from_base"`
	Version      Version  `json:"version"`
	Value        string   `json:"value"`
	Estimate     bool     `json:"estimate"`
	DependencyOn *Version `json:"dependency_on,omitempty"`
}

type CapturedReads struct {
	Reads []ReadDescriptor `json:"reads"`
}

func (c *CapturedReads) Add(read ReadDescriptor) {
	c.Reads = append(c.Reads, read)
}

type ValidationResult struct {
	Valid      bool           `json:"valid"`
	FailedKey  string         `json:"failed_key,omitempty"`
	Expected   ReadDescriptor `json:"expected,omitempty"`
	Observed   ReadDescriptor `json:"observed,omitempty"`
	Dependency *Version       `json:"dependency,omitempty"`
}
