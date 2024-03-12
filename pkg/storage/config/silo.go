package config

import (
	"fmt"
	"os"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
)

type SiloSchema struct {
	Device []*DeviceSchema `hcl:"device,block"`
}

type DeviceSchema struct {
	Name   string `hcl:"name,label"`
	Size   string `hcl:"size,attr"`
	Expose bool   `hcl:"expose,optional"`
}

func ReadSchema(path string) (*SiloSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	s := new(SiloSchema)
	return s, s.Decode(data)
}

func (s *SiloSchema) Decode(data []byte) error {
	file, diag := hclsyntax.ParseConfig(data, "", hcl.Pos{Line: 1, Column: 1})
	if diag.HasErrors() {
		return diag.Errs()[0]
	}

	diag = gohcl.DecodeBody(file.Body, nil, s)
	if diag.HasErrors() {
		return diag.Errs()[0]
	}

	return nil
}

func (s *SiloSchema) Encode() ([]byte, error) {
	f := hclwrite.NewEmptyFile()
	gohcl.EncodeIntoBody(s, f.Body())
	return f.Bytes(), nil
}
