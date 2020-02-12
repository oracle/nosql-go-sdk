//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// Package test provides configurations and utility functions for NoSQL client test.
package test

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/cloudsim"
)

var (
	config      *Config
	client      *nosqldb.Client
	interceptor Interceptor
)

// Config represents a test configuration.
type Config struct {
	// Mode specifies on which mode the tests run.
	// Available test modes are:
	//
	//   cloud    : test with the NoSQL cloud service
	//   cloudsim : test with the NoSQL cloud simulator
	//   onprem   : test with the on-premise NoSQL server
	//
	Mode string `json:"mode"`

	// Endpoint specifies an endpoint to use to connect to the Oracle NoSQL
	// database cloud service or, if on-premise, the Oracle NoSQL database proxy
	// server.
	Endpoint string `json:"endpoint"`

	// TablePrefix specifies a prefix for table names created in the tests.
	TablePrefix string `json:"tablePrefix"`

	// DropTablesOnTearDown specifies whether to drop the tables that were
	// created during testing on teardown of test suite.
	//
	// If not specified, the tables are kept after test.
	DropTablesOnTearDown bool `json:"dropTablesOnTearDown"`

	// Username and password are used to authenticate with the secure NoSQL server on-premise.
	//
	// These are only used for on-premise test.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// newConfig creates a test configuration object from the specified JSON file.
func newConfig(configFile string) (*Config, error) {
	if configFile == "" {
		return nil, errors.New("config file not specified")
	}

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configFile, err)
	}

	var cfg Config
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse configurations from file %s: %v", configFile, err)
	}

	return &cfg, nil
}

// IsCloud returns true if tests are configured to run against the NoSQL cloud
// service or cloud simulator, returns false otherwise.
func (cfg *Config) IsCloud() bool {
	return cfg != nil && (cfg.Mode == "cloud" || cfg.Mode == "cloudsim")
}

// IsOnPrem returns true if tests are configured to run against the on-premise
// NoSQL database servers, returns false otherwise.
func (cfg *Config) IsOnPrem() bool {
	return cfg != nil && cfg.Mode == "onprem"
}

// IsOnPremSecureStore returns true if tests are configured to run against
// the on-premise NoSQL database server that has security enabled, returns false otherwise.
func (cfg *Config) IsOnPremSecureStore() bool {
	if cfg == nil {
		return false
	}

	return cfg.IsOnPrem() && cfg.Username != "" && cfg.Password != ""
}

// createConfig creates a test configuration object from the JSON file specified
// on command line of the form:
//
//   testConfig=<path to JSON file>
//
func createConfig() (cfg *Config, err error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	var configFile string
	const key = "testConfig="
	for _, arg := range flag.Args() {
		if strings.HasPrefix(arg, key) {
			configFile = arg[len(key):]
			break
		}
	}

	if configFile == "" {
		return nil, errors.New("testConfig is not specified")
	}

	return newConfig(configFile)
}

// getConfig returns a test configuration.
//
// If there is a test configuration object already created, the configuration
// is returned, otherwise it creates a new configuration.
func getConfig() (*Config, error) {
	if config != nil {
		return config, nil
	}

	var err error
	config, err = createConfig()
	return config, err
}

// createClient creates a NoSQL client with the specified test configuration.
func createClient(cfg *Config) (*nosqldb.Client, error) {
	clientConfig := nosqldb.Config{
		Endpoint: cfg.Endpoint,
		Mode:     cfg.Mode,
	}

	switch cfg.Mode {
	case "cloudsim":
		clientConfig.AuthorizationProvider = &cloudsim.AccessTokenProvider{
			TenantID: "TestTenantId",
		}

	case "onprem":
		if cfg.Username != "" && cfg.Password != "" {
			clientConfig.Username = cfg.Username
			clientConfig.Password = []byte(cfg.Password)
		}

		// Accept any certificates presented by the server and any host name in that certificate.
		//
		// This is used for testing, not recommended in production.
		clientConfig.InsecureSkipVerify = true
	}

	var err error
	client, err = nosqldb.NewClient(clientConfig)
	if err != nil {
		return nil, err
	}

	if interceptor != nil {
		err = interceptor.OnSetupClient(client)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

// getClient returns a NoSQL client used for testing.
//
// If there is a test client already created, the client is returned, otherwise
// it creates a new client with the specified configuration.
func getClient(cfg *Config) (*nosqldb.Client, error) {
	if client != nil {
		return client, nil
	}

	var err error
	client, err = createClient(cfg)
	return client, err
}

// Interceptor represents an interceptor that used to inject customized
// procedures to setup NoSQL client, setup and teardown test resources.
//
// This is used for internal tests.
type Interceptor interface {
	// OnSetupClient sets up the specified NoSQL client for testing.
	OnSetupClient(client *nosqldb.Client) error

	// OnSetupTestSuite creates test resources before all tests run.
	OnSetupTestSuite() error

	// OnTearDownTestSuite releases test resources after all tests run.
	OnTearDownTestSuite() error
}

// SetInterceptor set the specified interceptor.
func SetInterceptor(i Interceptor) {
	interceptor = i
}

// IsCloud returns true if tests are configured to run against the NoSQL cloud
// service or clous simulator, returns false otherwise.
func IsCloud() bool {
	return config.IsCloud()
}

// IsOnPrem returns true if tests are configured to run against the on-premise
// NoSQL database servers, returns false otherwise.
func IsOnPrem() bool {
	return config.IsOnPrem()
}

// IsOnPremSecureStore returns true if tests are configured to run against
// the on-premise NoSQL database server that has security enabled, returns false otherwise.
func IsOnPremSecureStore() bool {
	return config.IsOnPremSecureStore()
}

const (
	// OkCreateTableTmpl is a template for generating table creation statement.
	// The table name should be provided when using the template.
	OkCreateTableTmpl = "create table %s (id integer, c1 string, c2 long, primary key(id))"

	// OkTimeout represents a valid value for operation request timeout.
	OkTimeout = 6 * time.Second

	// BadTimeout represents an invalid value for operation request timeout
	// that is less than 1 millisecond.
	BadTimeout = time.Millisecond - 1

	// WaitTimeout represents the timeout value that usually used in the
	// WaitForXXX operation.
	WaitTimeout = 15 * time.Second

	// MaxReadKBLimit represents the limit on the maximum read KB during an operation.
	MaxReadKBLimit = 2 * 1024

	// MaxWriteKBLimit represents the limit on the maximum write KB during an operation.
	MaxWriteKBLimit = 2 * 1024

	// MaxQuerySizeLimit represents the limit on a query string length.
	MaxQuerySizeLimit = 10 * 1024

	// MinQueryCost represents the minimum cost for a query operation.
	MinQueryCost = 2

	// MinReadKB represents the minimum read KB for a query operation
	MinReadKB = 1

	// The default interval between two tests.
	// This is used to avoid throttling errors during testing.
	defaultTestInterval = 500 * time.Millisecond

	// MaxDataSizeLimit represents the limit on data size for a row.
	// It is 512 KB.
	MaxDataSizeLimit = 512 * 1024

	// MaxBatchOpNumberLimit represents the limit on number of operations for a batch operation.
	MaxBatchOpNumberLimit = 50
)

var (
	// OkTableLimits represents a valid value for TableLimits.
	OkTableLimits = &nosqldb.TableLimits{ReadUnits: 2, WriteUnits: 2, StorageGB: 1}

	// BadTableLimits represents an invalid value for TableLimits.
	BadTableLimits = &nosqldb.TableLimits{}
)
