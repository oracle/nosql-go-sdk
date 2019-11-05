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
	"fmt"
	"runtime"
)

const (
	// Major, minor and patch versions for the SDK.
	major = 5
	minor = 0
	patch = 0

	// NoSQL cloud service version.
	serviceVersion = "V0"
	// URI for data service.
	DataServiceURI = "/V0/nosql/data"
	// URI for security service. This is used for on-premise only.
	SecurityServiceURI = "/V0/nosql/security"
)

var sdkVersion, userAgent string

// Sets sdkVersion and userAgent in package init function
func init() {
	sdkVersion = fmt.Sprintf("%d.%d.%d", major, minor, patch)
	// A sample User-Agent header: NoSQL-GoSDK/5.0.0 (go1.11; linux/amd64)
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
