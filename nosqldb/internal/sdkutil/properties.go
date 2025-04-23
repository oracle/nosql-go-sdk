//
// Copyright (c) 2019, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package sdkutil

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

// Properties is used to read and write properties files.
// The properties in the file must be written in the form:
//
//   name=value
//
type Properties struct {
	file  string
	props map[string]string
	err   error
	mu    sync.RWMutex
}

// NewProperties creates a Properties with the specified properties file.
func NewProperties(file string) (p *Properties, err error) {
	file, err = ExpandPath(file)
	if err != nil {
		return
	}

	if err = checkFile(file); err != nil {
		return
	}

	return &Properties{
		file:  file,
		props: make(map[string]string),
	}, nil
}

// Load reads properties from the file. If any errors occur during read,
// the error is reported in the Err() method.
//
// Empty lines and lines that start with the '#' mark are ignored.
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
		if idx <= 0 {
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

// Err reports any error occurs during Load().
func (p *Properties) Err() error {
	return p.err
}

// Save saves the properties to the file.
// The properties are sorted in lexical order according to property name when saved.
func (p *Properties) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	size := len(p.props)
	if size == 0 {
		return nil
	}

	keys := make([]string, 0, size)
	for k := range p.props {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		v := p.props[k]
		buf.WriteString(fmt.Sprintf("%s=%s\n", k, v))
	}

	tmpFile := fmt.Sprintf("%s.tmp", p.file)
	perm := os.FileMode(0644)
	if err := os.WriteFile(tmpFile, buf.Bytes(), perm); err != nil {
		return err
	}
	return os.Rename(tmpFile, p.file)
}

// Get reads the property associated with key.
func (p *Properties) Get(key string) (string, error) {
	p.mu.RLock()
	v, ok := p.props[key]
	p.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("cannot find property %q from file %s", key, p.file)
	}
	return v, nil
}

// Put writes the property value associated with key.
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

// ExpandPath cleans and expands the path if it contains a tilde, returns the
// expanded path or the input path as is if no expansion was performed.
func ExpandPath(filePath string) (string, error) {
	cleanedPath := path.Clean(filePath)
	expandedPath := cleanedPath
	if strings.HasPrefix(cleanedPath, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		expandedPath = path.Join(home, cleanedPath[1:])
	}
	return expandedPath, nil
}
