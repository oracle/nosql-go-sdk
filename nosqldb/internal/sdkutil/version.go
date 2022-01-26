//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package sdkutil provides internal utility functions for the SDK.
package sdkutil

import (
	"fmt"
	"runtime"
)

const (
	// Major, minor and patch versions for the SDK.
	major = 1
	minor = 2
	patch = 1

	// NoSQL service version.
	serviceVersion = "V2"

	// DataServiceURI represents the URI for data service.
	DataServiceURI = "/" + serviceVersion + "/nosql/data"

	// SecurityServiceURI represents the URI for security service.
	// This is used for on-premise only.
	SecurityServiceURI = "/" + serviceVersion + "/nosql/security"
)

var sdkVersion, userAgent string

// Sets sdkVersion and userAgent in package init function.
func init() {
	sdkVersion = fmt.Sprintf("%d.%d.%d", major, minor, patch)
	// A sample User-Agent header: NoSQL-GoSDK/1.1.0 (go1.13; linux/amd64)
	userAgent = fmt.Sprintf("NoSQL-GoSDK/%s (%s; %s/%s)",
		sdkVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

// SDKVersion returns the Oracle NoSQL Go SDK version.
func SDKVersion() string {
	return sdkVersion
}

// UserAgent returns a descriptive string that can be set in the "User-Agent"
// header of HTTP requests.
func UserAgent() string {
	return userAgent
}
