//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package sdkutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type PropTestSuite struct {
	suite.Suite
}

func TestProps(t *testing.T) {
	suite.Run(t, &PropTestSuite{})
}

func (suite *PropTestSuite) TestNewProperties() {
	tests := []struct {
		desc     string
		filePath string
		wantErr  bool
	}{
		{"empty file path", "", true},
		{"not exist file", "testdata/non_exist_file__", true},
		{"specify a directory for config file", "testdata", true},
		{"good file path", "testdata/test1.properties", false},
	}

	var msg string
	for i, r := range tests {
		msg = fmt.Sprintf("Test-%d (%s) NewProperties(%s) ", i+1, r.desc, r.filePath)
		_, err := NewProperties(r.filePath)
		if r.wantErr {
			suite.Errorf(err, msg+"should have failed")
		} else {
			suite.NoErrorf(err, msg+"got error %v", err)
		}
	}
}

func (suite *PropTestSuite) TestGet() {
	p, err := NewProperties("testdata/test1.properties")
	if !suite.NoErrorf(err, "NewProperties() got error %v", err) {
		return
	}

	p.Load()
	err = p.Err()
	if !suite.NoErrorf(err, "Load() got error %v", err) {
		return
	}

	tests := []struct {
		propName  string
		wantValue string
		wantErr   bool
	}{
		{"prop1", "value1", false},
		{"prop2", "new-value2", false},
		{"prop3", "value3", false},
		{"prop4", "", false},
		{"prop5", "", true},
	}

	for _, r := range tests {
		v, err := p.Get(r.propName)
		if r.wantErr {
			suite.Errorf(err, "Get(%s) should have failed", r.propName)
		} else {
			suite.NoErrorf(err, "Get(%s) got error %v", r.propName, err)
			suite.Equalf(r.wantValue, v, "Get(%s) got unexpected value", r.propName)
		}
	}
}

func (suite *PropTestSuite) TestPut() {
	f, err := ioutil.TempFile("", "go-driver-test-*.properties")
	if !suite.NoErrorf(err, "failed to create a temporary file for testing") {
		return
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	p, err := NewProperties(name)
	if !suite.NoErrorf(err, "NewProperties(%s) got error %v", name, err) {
		return
	}

	p.Load()
	err = p.Err()
	if !suite.NoErrorf(err, "Load() got error %v", err) {
		return
	}

	p.Save()
	fileInfo, err := os.Stat(name)
	if suite.NoErrorf(err, "os.Stat(%s) got error %v", name, err) {
		// Verify the file size is zero.
		suite.Equalf(int64(0), fileInfo.Size(), "got unexpected file size for %s", name)
	}

	// Put properties and save to the file.
	p.Put("name1", "value1")
	p.Put("name2", "value2")
	p.Put("name1", "new-value1")
	suite.Equalf(2, len(p.props), "Got unexpected number of map entries.")

	p.Save()
	fileInfo, err = os.Stat(name)
	if suite.NoErrorf(err, "os.Stat(%s) got error %v", name, err) {
		suite.Greaterf(fileInfo.Size(), int64(0), "got unexpected file size for %s", name)
	}

}

func (suite *PropTestSuite) TestConcurrency() {
	f, err := ioutil.TempFile("", "go-driver-test-*.properties")
	if !suite.NoErrorf(err, "failed to create a temporary file for testing") {
		return
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	p, err := NewProperties(name)
	if !suite.NoErrorf(err, "NewProperties(%s) got error %v", name, err) {
		return
	}

	// Put properties.
	var k, v string
	n := int32(10)
	for i := 1; i <= int(n); i++ {
		k = "name-" + strconv.Itoa(i)
		v = "value-" + strconv.Itoa(i)
		p.Put(k, v)
	}

	// Run concurrency test for 2 seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Put/Get properties concurrently.
	for i := 0; i < 6; i++ {
		go suite.getProp(ctx, p, &n)
		if i%2 == 0 {
			go suite.putProp(ctx, p, &n)
		}
	}

	<-ctx.Done()
}

func (suite *PropTestSuite) getProp(ctx context.Context, p *Properties, counter *int32) {
	var name, wantValue, gotValue, s string
	var err error
	var max int
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Randomly pick an index in the range [1, max].
			max = int(atomic.LoadInt32(counter))
			s = strconv.Itoa(1 + rand.Intn(max))
			name = "name-" + s
			wantValue = "value-" + s
			gotValue, err = p.Get(name)
			if suite.NoErrorf(err, "Get(%s) got error %v", name, err) {
				suite.Equalf(wantValue, gotValue, "Get(%s) got unexpected value", name)
			}
		}
	}
}

func (suite *PropTestSuite) putProp(ctx context.Context, p *Properties, counter *int32) {
	var name, value, s string
	var i int32
	for {
		select {
		case <-ctx.Done():
			return
		default:
			i = atomic.LoadInt32(counter) + 1
			s = strconv.Itoa(int(i))
			name = "name-" + s
			value = "value-" + s
			// Put and then increase the counter.
			p.Put(name, value)
			atomic.StoreInt32(counter, i)
		}
	}
}
