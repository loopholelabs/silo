package config

import (
	"errors"
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
	Device []*DeviceSchema `hcl:"device,block"`
}

type DeviceSchema struct {
	Name           string                 `hcl:"name,label"`
	Size           string                 `hcl:"size,attr"`
	System         string                 `hcl:"system,attr"`
	BlockSize      string                 `hcl:"blocksize,optional"`
	Expose         bool                   `hcl:"expose,optional"`
	Location       string                 `hcl:"location,optional"`
	ROSource       *DeviceSchema          `hcl:"source,block"`
	ROSourceHashes string                 `hcl:"sourcehashes,optional"`
	ROSourceShared bool                   `hcl:"sourceshared,optional"`
	LoadBinLog     string                 `hcl:"loadbinlog,optional"`
	Binlog         string                 `hcl:"binlog,optional"`
	PageServerPID  int                    `hcl:"pid,optional"`
	Sync           *SyncS3Schema          `hcl:"sync,block"`
	Migration      *MigrationConfigSchema `hcl:"migration,block"`
	WriteCache     *WriteCacheSchema      `hcl:"writecache,block"`
}

type WriteCacheSchema struct {
	MinSize     string `hcl:"minsize,attr"`
	MaxSize     string `hcl:"maxsize,attr"`
	FlushPeriod string `hcl:"flushperiod,attr"`
}

type MigrationConfigSchema struct {
	AnyOrder bool `hcl:"anyorder,attr"`
}

type SyncConfigSchema struct {
	OnlyDirty   bool   `hcl:"onlydirty,attr"`
	BlockShift  int    `hcl:"blockshift,attr"`
	MaxAge      string `hcl:"maxage,attr"`
	MinChanged  int    `hcl:"minchanged,attr"`
	CheckPeriod string `hcl:"checkperiod,attr"`
	Limit       int    `hcl:"limit,attr"`
	Concurrency int    `hcl:"concurrency,attr"`
}

type SyncS3Schema struct {
	Secure          bool              `hcl:"secure,attr"`
	AccessKey       string            `hcl:"accesskey,attr"`
	SecretKey       string            `hcl:"secretkey,attr"`
	Endpoint        string            `hcl:"endpoint,attr"`
	Bucket          string            `hcl:"bucket,attr"`
	Prefix          string            `hcl:"prefix,optional"`
	GrabPrefix      string            `hcl:"grabprefix,optional"`
	Config          *SyncConfigSchema `hcl:"config,block"`
	AutoStart       bool              `hcl:"autostart,attr"`
	GrabConcurrency int               `hcl:"grabconcurrency,attr"`
}

func ParseByteValue(val string) int64 {
	// Parse the size string
	multiplier := int64(1)
	s := strings.Trim(strings.ToLower(val), " \t\r\n")
	if s == "" {
		return 0
	}

	suffix := s[len(s)-1:] // Get the last byte
	switch suffix {
	case "b":
		multiplier = 1
		s = s[:len(s)-1]
	case "k":
		multiplier = 1024
		s = s[:len(s)-1]
	case "m":
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case "g":
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return i * multiplier
}

func (ds *DeviceSchema) ByteSize() int64 {
	return ParseByteValue(ds.Size)
}

func (ds *DeviceSchema) ByteBlockSize() int64 {
	return ParseByteValue(ds.BlockSize)
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

func (ds *DeviceSchema) Encode() []byte {
	f := hclwrite.NewEmptyFile()
	gohcl.EncodeIntoBody(ds, f.Body())
	return f.Bytes()
}

func (ds *DeviceSchema) EncodeAsBlock() []byte {
	f := hclwrite.NewEmptyFile()
	block := gohcl.EncodeAsBlock(ds, "device")
	f.Body().AppendBlock(block)
	return f.Bytes()
}

func DecodeDeviceFromBlock(schema string) (*DeviceSchema, error) {
	sf := &SiloSchema{}
	err := sf.Decode([]byte(schema))
	if err != nil {
		return nil, err
	}
	if len(sf.Device) != 1 {
		return nil, errors.New("more than one device in schema")
	}
	return sf.Device[0], nil
}

func (ds *DeviceSchema) Decode(schema string) error {
	file, diag := hclsyntax.ParseConfig([]byte(schema), "", hcl.Pos{Line: 1, Column: 1})
	if diag.HasErrors() {
		return diag.Errs()[0]
	}

	diag = gohcl.DecodeBody(file.Body, nil, ds)
	if diag.HasErrors() {
		return diag.Errs()[0]
	}

	return nil
}
