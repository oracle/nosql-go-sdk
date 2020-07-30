//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/cloudsim"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/kvstore"
	"github.com/oracle/nosql-go-sdk/nosqldb/common"
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
// not specified.
type Config struct {
	// Endpoint specifies the Oracle NoSQL server endpoint that clients connect to.
	// It is required when connect to the Oracle NoSQL cloud simulator or
	// the on-premise Oracle NoSQL server.
	// It must include the target address, and may include protocol and port.
	// The syntax is:
	//
	//   [http[s]://]host[:port]
	//
	// For example, these are valid endpoints:
	//
	//   localhost:8080
	//   http://localhost:8080
	//   https://localhost:8090
	//
	// If port is omitted, the endpoint defaults to 443.
	// If protocol is omitted, the endpoint uses https if the port is 443, and
	// http in all other cases.
	Endpoint string

	// Region specifies the region for the Oracle NoSQL cloud service that clients connect to.
	// Region takes precedence over the "region" property that may be specified
	// in the OCI configuration file which is ~/.oci/config by default.
	//
	// This is used for cloud service only.
	Region common.Region

	// Mode specifies the configuration mode for client, which is one of:
	//
	//   "cloud": for connecting to a NoSQL cloud service
	//   "cloudsim": for connecting to a local cloud simulator
	//   "onprem": for connecting to an on-premise NoSQL server
	//
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
	//   use iam.SignatureProvider for NoSQL cloud service that uses OCI IAM.
	//   use kvstore.AccessTokenProvider for the secure NoSQL servers on-premise.
	//
	AuthorizationProvider

	// RetryHandler specifies a handler used to handle operation retries.
	RetryHandler

	host     string
	port     string
	protocol string

	httpClient *httputil.HTTPClient
}

func (c *Config) validate() error {
	c.Mode = strings.ToLower(c.Mode)
	switch c.Mode {
	case "", "cloud":
	case "cloudsim", "onprem":
		if len(c.Endpoint) == 0 {
			return fmt.Errorf("Endpoint must be specified")
		}
	default:
		return fmt.Errorf("the specified configuration mode %q is not supported", c.Mode)
	}

	if len(c.Endpoint) > 0 && len(c.Region) > 0 {
		return fmt.Errorf("cannot have both Endpoint and Region specified")
	}

	return nil
}

func (c *Config) setDefaults() (err error) {
	err = c.validate()
	if err != nil {
		return
	}

	// Set a default logger.
	if c.Logger == nil && !c.DisableLogging {
		c.Logger = logger.DefaultLogger
	}

	// Set a default RetryHandler if not specified.
	if c.RetryHandler == nil {
		c.RetryHandler, err = NewDefaultRetryHandler(5, time.Second)
		if err != nil {
			return
		}
	}

	// Check the specified endpoint and set default authorization provider
	// when connect to cloud simulator or on-premise server.
	if c.Mode == "cloudsim" || c.Mode == "onprem" {
		err = c.parseEndpoint()
		if err != nil {
			return err
		}

		if c.Mode == "cloudsim" {
			// Set a default AuthorizationProvider if not specified.
			if c.AuthorizationProvider == nil {
				c.AuthorizationProvider = &cloudsim.AccessTokenProvider{
					TenantID: "ExampleTenantId",
				}
			}

			return nil
		}

		// Create an AuthorizationProvider for the secure on-premise NoSQL server,
		// for non-secure on-premise NoSQL server, the AuthorizationProvider
		// is not required.
		if c.AuthorizationProvider == nil && len(c.Username) > 0 && len(c.Password) > 0 {
			c.httpClient, err = httputil.NewHTTPClient(c.HTTPConfig)
			if err != nil {
				return err
			}

			options := auth.ProviderOptions{
				Timeout:    c.DefaultSecurityInfoTimeout(),
				Logger:     c.Logger,
				HTTPClient: c.httpClient,
			}

			c.AuthorizationProvider, err = kvstore.NewAccessTokenProvider(c.Username, c.Password, options)
			if err != nil {
				return err
			}
		}

		// Set service endpoint for the on-premise NoSQL server.
		if atp, ok := c.AuthorizationProvider.(*kvstore.AccessTokenProvider); ok {
			atp.SetEndpoint(c.Endpoint)

			// If user provides an AccessTokenProvider that does not set an
			// http client, create one and set it for the provider.
			if atp.GetHTTPClient() == nil {
				if c.httpClient == nil {
					c.httpClient, err = httputil.NewHTTPClient(c.HTTPConfig)
					if err != nil {
						return err
					}
				}

				atp.SetHTTPClient(c.httpClient)
			}
		}

		return nil
	}

	// Set default signature provider for cloud service if not specified,
	// then check the region or endpoint.
	if c.AuthorizationProvider == nil {
		c.AuthorizationProvider, err = iam.NewSignatureProvider()
		if err != nil {
			return err
		}
	}

	// When connect to cloud service, look for Region or Endpoint in order:
	//
	//   1. use Config.Region if it is specified
	//   2. use the "region" field from OCI configuration file if it is specified
	//   3. use Config.Endpoint if it is specified
	//
	if len(c.Region) == 0 {
		var regionID string
		if sp, ok := c.AuthorizationProvider.(*iam.SignatureProvider); ok {
			if profile := sp.Profile(); profile != nil {
				regionID, _ = profile.Region()
			}
		}

		switch {
		// region is specified in OCI config file
		case len(regionID) > 0:
			c.Region = common.Region(regionID)
		// neither region nor endpoint is specified
		case len(c.Endpoint) == 0:
			return fmt.Errorf("Region must be specified")
		}
	}

	err = c.parseEndpoint()
	return
}

// parseEndpoint returns endpoint for the Region if specified, or tries to parse
// the specified Endpoint. It returns an error if the specified region is not
// recognized or the specified Endpoint does not conform to the syntax:
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
	if len(string(c.Region)) > 0 {
		c.host, err = c.Region.Endpoint()
		if err != nil {
			return err
		}

		c.protocol = "https"
		c.port = "443"
		c.Endpoint = c.protocol + "://" + c.host + ":" + c.port
		c.HTTPConfig.UseHTTPS = true
		return nil
	}

	c.protocol, c.host, c.port, err = parseEndpoint(c.Endpoint)
	if err != nil {
		return
	}

	c.Endpoint = c.protocol + "://" + c.host + ":" + c.port
	c.HTTPConfig.UseHTTPS = c.protocol == "https"
	return nil
}

// IsCloud returns whether the configuration is used for cloud service.
func (c *Config) IsCloud() bool {
	return c.Mode == "" || strings.EqualFold(c.Mode, "cloud")
}

// IsCloudSim returns whether the configuration is used for cloud simulator.
func (c *Config) IsCloudSim() bool {
	return strings.EqualFold(c.Mode, "cloudsim")
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
