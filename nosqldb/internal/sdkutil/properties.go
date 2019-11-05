//
//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//
//

// This file implements reading and writing Java style properties files.

package sdkutil

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
)

type Properties struct {
	file  string
	props map[string]string
	err   error
	mu    sync.RWMutex
}

func NewProperties(filePath string) (p *Properties, err error) {
	if err = checkFile(filePath); err != nil {
		return
	}
	p = &Properties{
		file:  filePath,
		props: make(map[string]string),
	}
	return
}

func (p *Properties) Load() {
	// reset error
	p.err = nil
	p.mu.RLock()
	defer p.mu.RUnlock()

	f, err := os.Open(p.file)
	if err != nil {
		p.err = err
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 || len(line) <= idx+1 {
			continue
		}
		if key := strings.TrimSpace(line[:idx]); len(key) > 0 {
			value := strings.TrimSpace(line[idx+1:])
			p.props[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		p.err = err
		return
	}
}

func (p *Properties) Err() error {
	return p.err
}

func (p *Properties) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	size := len(p.props)
	if size == 0 {
		return nil
	}

	keys := make([]string, size)
	i := 0
	for k, _ := range p.props {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		v := p.props[k]
		buf.WriteString(fmt.Sprintf("%s=%s\n", k, v))
	}

	tmpFile := fmt.Sprintf("%s.tmp", p.file)
	perm := os.FileMode(0644)
	if err := ioutil.WriteFile(tmpFile, buf.Bytes(), perm); err != nil {
		return err
	}
	return os.Rename(tmpFile, p.file)
}

func (p *Properties) Get(key string) (string, error) {
	p.mu.RLock()
	v, ok := p.props[key]
	p.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("cannot find property %q from file %s", key, p.file)
	}
	return v, nil
}

func (p *Properties) Put(key, value string) {
	p.mu.Lock()
	p.props[key] = value
	p.mu.Unlock()
}

func checkFile(file string) error {
	if file == "" {
		return errors.New("file path must be non-empty")
	}
	fileInfo, err := os.Stat(file)
	if err != nil {
		return err
	}
	if !fileInfo.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", file)
	}
	return nil
}
