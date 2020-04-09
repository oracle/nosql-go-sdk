//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// Package httputil provides utility functions used for HTTP clients.
package httputil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"
)

// HTTPClient represents an HTTP client.
// It is used to handle connections, send HTTP requests to and receive HTTP
// responses from server. It is implemented based on http.client, providing
// convenient configuration options to take control of client connections.
//
// The underlying http.client's Transport maintains internal state, such as
// cached TCP connections, which can be reused. So an HTTPClient can handle
// multiple client connections, it should be reused instead of created as
// needed.
type HTTPClient struct {
	// client represents the underlying http.client.
	client *http.Client
}

// NewHTTPClient creates an HTTPClient using the specified configurations.
func NewHTTPClient(cfg HTTPConfig) (*HTTPClient, error) {
	hc := &HTTPClient{}
	// Set default values for Transport, the values will later be overwritten by
	// the provided configurations if specified.
	tr := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		DisableKeepAlives:     false,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if cfg.UseProxyFromEnv {
		tr.Proxy = http.ProxyFromEnvironment
	} else if cfg.ProxyURL != "" {
		pu, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, err
		}
		tr.Proxy = http.ProxyURL(pu)
		if cfg.ProxyUsername != "" && cfg.ProxyPassword != "" {
			auth := BasicAuth(cfg.ProxyUsername, []byte(cfg.ProxyPassword))
			tr.ProxyConnectHeader = http.Header{}
			tr.ProxyConnectHeader.Add("Proxy-Authorization", auth)
		}
	}

	if cfg.MaxIdleConns != 0 {
		tr.MaxIdleConns = cfg.MaxIdleConns
	}
	if cfg.MaxIdleConnsPerHost != 0 {
		tr.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	}
	if cfg.IdleConnTimeout != 0 {
		tr.IdleConnTimeout = cfg.IdleConnTimeout
	}

	sessionTimeout := 30 * time.Second

	if cfg.UseHTTPS {
		rootCAs, _ := x509.SystemCertPool()
		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if cfg.InsecureSkipVerify == false && cfg.CertPath != "" {
			certs, err := ioutil.ReadFile(cfg.CertPath)
			if err != nil {
				return nil, err
			}
			ok := rootCAs.AppendCertsFromPEM(certs)
			if !ok {
				return nil, fmt.Errorf("no valid PEM certs found in %s", cfg.CertPath)
			}
		}
		if cfg.SslSessionTimeout != 0 {
			sessionTimeout = cfg.SslSessionTimeout
		}
		tr.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify,
			RootCAs:            rootCAs,
			ServerName:         cfg.ServerName,
		}
	}

	tr.DialContext = (&net.Dialer{
		Timeout:   sessionTimeout,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext

	hc.client = &http.Client{Transport: tr}
	return hc, nil
}

// Do sends an HTTP request and returns an HTTP response.
// It implements the RequestExecutor interface.
func (hc *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	return hc.client.Do(req)
}

// DefaultHTTPClient is a default HTTPClient instance that is ready to use.
var DefaultHTTPClient = &HTTPClient{
	client: http.DefaultClient,
}

// HTTPConfig contains parameters used to configure HTTPClient.
type HTTPConfig struct {
	// UseHTTPS indicates if HTTPS is used.
	UseHTTPS bool

	// ProxyURL specifies an HTTP proxy server URL.
	// If specified, all transports go through the proxy server.
	ProxyURL string

	// ProxyUsername specifies the username used to authenticate with HTTP proxy
	// server if required.
	ProxyUsername string

	// ProxyPassword specifies the password used to authenticate with HTTP proxy
	// server if required.
	ProxyPassword string

	// UseProxyFromEnv indicates whether to use the proxy server that is set by
	// the environment variables HTTP_PROXY, HTTPS_PROXY and NO_PROXY
	// (or the lowercase versions thereof).
	// If UseProxyFromEnv is true, it takes precedence over the ProxyURL
	// parameter.
	UseProxyFromEnv bool

	// MaxIdleConns controls the maximum number of idle (keep-alive) connections
	// across all hosts.
	// The default value is 100.
	MaxIdleConns int

	// MaxIdleConnsPerHost controls the maximum idle (keep-alive) connections
	// to keep per-host.
	// The default value is 100.
	MaxIdleConnsPerHost int

	// IdleConnTimeout is the maximum amount of time an idle (keep-alive)
	// connection will remain idle before closing itself.
	// The default is 90 seconds.
	IdleConnTimeout time.Duration

	// SslSessionTimeout is the timeout value for an SSL session.
	// The default is 30 seconds.
	SslSessionTimeout time.Duration

	// InsecureSkipVerify controls whether a client verifies the server's
	// certificate chain and host name.
	// If InsecureSkipVerify is true, TLS accepts any certificate presented by
	// the server and any host name in that certificate.
	// In this mode, TLS is susceptible to man-in-the-middle attacks.
	InsecureSkipVerify bool

	// CertPath specifies the path to a pem-encoded certificate file.
	// Certificates in this file will be used in addition to system certificates.
	// This field is typically used for local self-signed certificates.
	// If InsecureSkipVerify is true, this field is ignored.
	CertPath string

	// ServerName is used to verify the hostname for self-signed certificates.
	// This field is only used if CertPath is nonempty, and is typically set
	// to the "CN" subject value from the certificate specified by CertPath.
	// If InsecureSkipVerify is true, this field is ignored.
	ServerName string

	// TODO:
	// CipherSuites	   []uint16
}
