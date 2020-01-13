//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"context"
	"net/http"
)

// This file exports functions/methods that are used in test codes.

type HandleResponse func(httpResp *http.Response, req Request) (Result, error)

func (c *Client) SetResponseHandler(fn HandleResponse) {
	c.handleResponse = fn
}

func (c *Client) ProcessRequest(req Request) (data []byte, err error) {
	return c.processRequest(req)
}

func (c *Client) DoExecute(ctx context.Context, req Request, data []byte) (Result, error) {
	return c.doExecute(ctx, req, data)
}

func (p *PreparedStatement) GetStatement() []byte {
	return p.statement
}
