package client

import "fmt"

type PatchOp int8

const (
	PatchOpAdd PatchOp = iota
	PatchOpReplace
	PatchOpRemove
)

func (p PatchOp) String() string {
	switch p {
	case PatchOpAdd:
		return "add"
	case PatchOpReplace:
		return "replace"
	case PatchOpRemove:
		return "remove"
	default:
		panic(fmt.Sprintf("k8s: unknown patch operation: %d", p))
	}
}

type PatchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

func NewLabelPatchStringValue(patchOp PatchOp, label, value string) PatchStringValue {
	return PatchStringValue{
		Op:    patchOp.String(),
		Path:  "/metadata/labels/" + label,
		Value: value,
	}
}
