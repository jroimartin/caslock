// Copyright 2015 The caslock Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package caslock implements a row lock mechanism for cassandra, based on
lightweight transactions. It allows to lock an arbitrary number of rows,
protecting them from concurrent access.

Usage:

	l, err := caslock.Acquire(session, "keyspace", "table",
		30 * time.Second, "rowKey1", "rowKey2")
	if err != nil {
		return err
	}
	defer l.Release()
*/
package caslock
