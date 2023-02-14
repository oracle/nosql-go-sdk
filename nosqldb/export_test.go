//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"context"
	"net/http"
)

// This file exports functions/methods that are used in test codes.

type HandleResponse func(httpResp *http.Response, req Request, serialVerUsed int16) (Result, error)

func (c *Client) SetResponseHandler(fn HandleResponse) {
	c.handleResponse = fn
}

func (c *Client) ProcessRequest(req Request) (data []byte, serialVerUsed int16, err error) {
	return c.processRequest(req)
}

func (c *Client) DoExecute(ctx context.Context, req Request, data []byte, serialVerUsed int16) (Result, error) {
	return c.doExecute(ctx, req, data, serialVerUsed)
}

func (p *PreparedStatement) GetStatement() []byte {
	return p.statement
}
