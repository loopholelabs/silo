package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
)

type SiloSchema struct {
	Device   []*DeviceSchema   `hcl:"device,block"`
	Location []*LocationSchema `hcl:"location,block"`
}

type DeviceSchema struct {
	Name      string `hcl:"name,label"`
	Size      string `hcl:"size,attr"`
	System    string `hcl:"system,attr"`
	BlockSize int    `hcl:"blocksize,optional"`
	Expose    bool   `hcl:"expose,optional"`
	Location  string `hcl:"location,optional"`
}

type LocationSchema struct {
	Name     string `hcl:"name,label"`
	System   string `hcl:"system,optional"`
	Location string `hcl:"location,optional"`
}

func (ds *DeviceSchema) ByteSize() int {
	// Parse the size string
	multiplier := 1
	s := strings.Trim(strings.ToLower(ds.Size), " \t\r\n")
	if strings.HasSuffix(s, "k") {
		multiplier = 1024
		s = s[:len(s)-1]
	} else if strings.HasSuffix(s, "m") {
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	} else if strings.HasSuffix(s, "g") {
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return int(i) * multiplier
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
