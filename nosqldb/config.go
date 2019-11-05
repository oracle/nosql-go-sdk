//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

const (
	// The maximum number of bytes allowed for the request content.
	// Payloads that exceed this value will result in an IllegalArgument error.
	// This is a currently set to 1MB, cannot be configured by user.
	maxContentLength = 1024 * 1024

	// The default timeout value for requests.
	// This applies to any requests other than TableRequest.
	defaultRequestTimeout = 5 * time.Second

	// The default timeout value for TableRequest.
	defaultTableRequestTimeout = 10 * time.Second

	// The default timeout value for retrieving security information such as
	// access tokens from authorization service.
	// This specifies a period of time waiting for security information to be available.
	defaultSecurityInfoTimeout = 10 * time.Second

	// The default Consistency value.
	defaultConsistency = types.Eventual
)

// Config represents a group of configuration parameters for a Client.
//
// When creating a Client, the Config instance is copied so modifications on the
// instance have no effect on the existing Client which is immutable.
//
// Most of the configuration parameters are optional and have default values if
// not specified. The only required parameter is the Endpoint.
type Config struct {
	// Endpoint specifies the NoSQL service endpoint that client connects to.
	// It is required.
	// It must include the target address, and may include protocol and port.
	// The syntax is:
	//
	//   [http[s]://]host[:port]
	//
	// For example, these are valid endpoints:
	//
	//   ndcs.uscom-east-1.oraclecloud.com
	//   https://ndcs.eucom-central-1.oraclecloud.com:443
	//   localhost:8080
	//
	// If port is omitted, the endpoint defaults to 443.
	// If protocol is omitted, the endpoint uses https if the port is 443, and
	// http in all other cases.
	Endpoint string

	// Mode specifies the configuration mode for client, which is either "cloud"
	// or "onprem" representing the client is configured for connecting to a
	// NoSQL cloud service or on-premise NoSQL server respectively.
	// If not set, the "cloud" mode is used by default.
	Mode string

	// Username specifies the user that used to authenticate with the server.
	// This is only used for on-premise NoSQL server that configured with security.
	Username string

	// Password specifies the password for user that used to authenticate with the server.
	// This is only used for on-premise NoSQL server that configured with security.
	Password []byte

	// Configurations for requests.
	RequestConfig

	// Configurations for HTTP client.
	httputil.HTTPConfig

	// Configurations for logging.
	LoggingConfig

	// Authorization provider.
	// If not specified, use the default authorization provider depending on the
	// configuration mode:
	//
	//   use idcs.AccessTokenProvider for NoSQL cloud service that authorizes requests by IDCS.
	//   use kvstore.AccessTokenProvider for the secure NoSQL servers on-premise.
	//
	AuthorizationProvider

	// RetryHandler specifies a handler used to handle operation retries.
	RetryHandler

	host     string
	port     string
	protocol string
}

// parseEndpoint tries to parse the specified Endpoint, returns an error if
// Endpoint does not conform to the syntax:
//
//   [http[s]://]host[:port]
//
// The following rules are applied to the Endpoint:
//
// 1. If protocol and port are both omitted, the Endpoint uses https with port 443.
//
// 2. If port is omitted, the Endpoint uses 443 for https, or 8080 for http.
//
// 3. If protocol is omitted, the Endpoint uses https if the port is 443, and
// http in all other cases.
func (c *Config) parseEndpoint() (err error) {
	c.protocol, c.host, c.port, err = parseEndpoint(c.Endpoint)
	if err != nil {
		return
	}

	c.Endpoint = c.protocol + "://" + c.host + ":" + c.port
	return nil
}

// IsCloudMode returns whether the configuration is used for cloud service.
func (c *Config) IsCloudMode() bool {
	return c.Mode == "" || strings.EqualFold(c.Mode, "cloud")
}

func parseEndpoint(endpoint string) (protocol, host, port string, err error) {
	if endpoint == "" {
		err = errors.New("Endpoint must be specified")
		return
	}

	if idx := strings.Index(endpoint, "://"); idx == -1 {
		host = endpoint
	} else {
		protocol = strings.ToLower(endpoint[:idx])
		if protocol != "https" && protocol != "http" {
			return "", "", "", fmt.Errorf("the specified protocol %q is not supported. "+
				"Must use \"https\" or \"http\"", protocol)
		}
		host = endpoint[idx+3:]
	}

	// Strip the ending slashes.
	if strings.HasSuffix(host, "/") {
		host = strings.TrimRightFunc(host, func(r rune) bool {
			return r == '/'
		})
	}

	bracket := strings.IndexByte(host, ']')
	colon := strings.LastIndexByte(host, ':')
	if colon > bracket {
		host, port, err = net.SplitHostPort(host)
		if err != nil {
			return "", "", "", err
		}
		if port != "" {
			portNum, err := strconv.Atoi(port)
			if err != nil || portNum < 0 {
				return "", "", "", fmt.Errorf("invalid port number %s", port)
			}
		}
	}

	if host == "" {
		return "", "", "", fmt.Errorf("invalid endpoint %q", endpoint)
	}

	switch {
	case protocol == "" && port == "":
		protocol = "https"
		port = "443"

	case protocol == "":
		if port == "443" {
			protocol = "https"
		} else {
			protocol = "http"
		}

	case port == "":
		if protocol == "https" {
			port = "443"
		} else {
			port = "8080"
		}
	}

	return
}

// RequestConfig represents a group of configuration parameters for requests.
type RequestConfig struct {
	// RequestTimeout specifies a timeout value for requests.
	// This applies to any requests other than TableRequest.
	// If set, it must be greater than or equal to 1 millisecond.
	RequestTimeout time.Duration

	// TableRequestTimeout specifies a timeout value for TableRequest.
	// If set, it must be greater than or equal to 1 millisecond.
	TableRequestTimeout time.Duration

	// SecurityInfoTimeout specifies a timeout value for retrieving security
	// information such as access tokens from authorization service.
	// This specifies a period of time waiting for security information to be available.
	// If set, it must be greater than or equal to 1 millisecond.
	SecurityInfoTimeout time.Duration

	// Consistency specifies a Consistency value for read requests, which
	// include GetRequest and QueryRequest.
	// If set, it must be either types.Eventual or types.Absolute.
	Consistency types.Consistency
}

// DefaultRequestTimeout returns the default timeout value for requests.
// If there is no configured timeout or it is configured as 0, a default value
// (defaultRequestTimeout) of 5 seconds is used.
func (r *RequestConfig) DefaultRequestTimeout() time.Duration {
	if r == nil || r.RequestTimeout == 0 {
		return defaultRequestTimeout
	}
	return r.RequestTimeout
}

// DefaultTableRequestTimeout returns the default timeout value for table
// requests. If there is no configured timeout or it is configured as 0, a
// default value (defaultTableRequestTimeout) of 10 seconds is used.
func (r *RequestConfig) DefaultTableRequestTimeout() time.Duration {
	if r == nil || r.TableRequestTimeout == 0 {
		return defaultTableRequestTimeout
	}
	return r.TableRequestTimeout
}

// DefaultSecurityInfoTimeout returns the default timeout value while waiting
// for security information to be available. If there is no configured timeout
// or it is configured as 0, a default value (defaultSecurityInfoTimeout) of 10
// seconds is used.
func (r *RequestConfig) DefaultSecurityInfoTimeout() time.Duration {
	if r == nil || r.SecurityInfoTimeout == 0 {
		return defaultSecurityInfoTimeout
	}
	return r.SecurityInfoTimeout
}

// DefaultConsistency returns the default Consistency value. If there is a
// configured Consistency it is returned. Otherwise a default value
// (defaultConsistency) of types.Eventual is used.
func (r *RequestConfig) DefaultConsistency() types.Consistency {
	if r == nil || r.Consistency == 0 {
		return defaultConsistency
	}
	return r.Consistency
}

// LoggingConfig represents logging configurations.
type LoggingConfig struct {

	// Configurations for the logger.
	// If this is not set, use logger.DefaultLogger unless DisableLogging is set.
	*logger.Logger

	// DisableLogging represents whether logging is disabled.
	DisableLogging bool
}
