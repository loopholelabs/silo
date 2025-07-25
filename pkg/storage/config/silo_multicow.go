package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

const SuffixOverlay = ".overlay"
const SuffixState = ".state"

type CowOverlay struct {
	Name     string
	System   string
	Location string
	Shared   bool
}

// Adds a COW overlay to this device
func (ds *DeviceSchema) AddOverlay(cow *CowOverlay) {
	rosrc := ds.ROSource

	ds.ROSource = &DeviceSchema{
		Name:           cow.Name,
		Size:           ds.Size,
		BlockSize:      ds.BlockSize,
		Location:       ds.Location,
		System:         ds.System,
		ROSource:       rosrc,
		ROSourceShared: ds.ROSourceShared,
	}

	// Now reconfig the base
	ds.System = cow.System
	ds.Location = cow.Location
	ds.ROSourceShared = cow.Shared
}

// Remove the outermost overlay, and return it
func (ds *DeviceSchema) RemoveOverlay() (*CowOverlay, error) {
	if ds.ROSource == nil {
		return nil, errors.New("no such layer")
	}
	system := ds.System
	location := ds.Location
	sourceShared := ds.ROSourceShared
	cowName := ds.ROSource.Name

	// Now do adjustment to extract the most recent layer.
	ds.System = ds.ROSource.System
	ds.Location = ds.ROSource.Location
	ds.ROSourceShared = ds.ROSource.ROSourceShared
	ds.ROSource = ds.ROSource.ROSource

	return &CowOverlay{
		Name:     cowName,
		System:   system,
		Location: location,
		Shared:   sourceShared}, nil
}

// Add overlays from a directory
func (ds *DeviceSchema) AddOverlays(dir string, prefix string, system string, shared bool) error {
	e, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	overlays := make(map[string]bool)
	states := make(map[string]bool)
	names := make([]string, 0)

	for _, n := range e {
		if !n.IsDir() && strings.HasPrefix(n.Name(), prefix) {
			if strings.HasSuffix(n.Name(), SuffixOverlay) {
				name := n.Name()[:len(n.Name())-len(SuffixOverlay)]
				overlays[name] = true
				names = append(names, name)
			} else if strings.HasSuffix(n.Name(), SuffixState) {
				name := n.Name()[:len(n.Name())-len(SuffixState)]
				states[name] = true
			}
		}
	}

	sort.Slice(names, func(i int, j int) bool {
		return names[i] < names[j]
	})

	// Now go through adding them...
	for _, n := range names {
		ds.AddOverlay(&CowOverlay{
			Name:     fmt.Sprintf("%s%s", n, SuffixState),
			Location: fmt.Sprintf("%s%s", n, SuffixOverlay),
			System:   system,
			Shared:   shared,
		})
	}

	return err
}
