// Copyright 2015 The caslock Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package caslock

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

var (
	// LockColumn is the name of the column used to control row locks. It
	// must added to the desired column family before using caslock.
	LockColumn = "[lock]"

	// RetryTime is time beteween retries when a row has been lock by another
	// process.
	RetryTime = 500 * time.Millisecond

	// Log is the logger used to register warnings and info messages. If it is nil,
	// no messages will be logged.
	Log *log.Logger
)

// Lock represents a cassandra's row lock.
type Lock struct {
	lockID   gocql.UUID
	keyspace string
	table    string
	rowKeys  []interface{}
	ttl      time.Duration
	timeout  time.Duration
	session  *gocql.Session
}

var errTimeout = errors.New("cannot satisfy timeout value")

// Acquire locks the rows in rowKeys on the table keyspace.table. The parameter
// timeout defines the minimum time that the rows will locked before being
// automatically released.
func Acquire(session *gocql.Session, keyspace, table string, timeout time.Duration, rowKeys ...interface{}) (*Lock, error) {
	l := &Lock{
		lockID:   gocql.TimeUUID(),
		keyspace: keyspace,
		table:    table,
		rowKeys:  rowKeys,
		ttl:      timeout * 2,
		timeout:  timeout,
		session:  session,
	}

loop:
	for {
		switch err := l.acquireLoop(); err {
		case nil:
			break loop
		case errTimeout:
			continue loop
		default:
			return nil, err
		}
	}
	return l, nil
}

func (l *Lock) acquireLoop() error {
	var maxTime time.Time
	for i, k := range l.rowKeys {
		for {
			if i == 0 {
				maxTime = time.Now().Add(l.ttl)
			} else if maxTime.Sub(time.Now()) < l.timeout {
				logf("cannot satisfy timeout, restarting acquire")
				l.Release()
				return errTimeout
			}

			applied, err := l.lockRow(k)
			if err != nil {
				l.Release()
				return err
			}
			if applied {
				break
			}

			// check if the rows really exist before retrying
			if err := l.checkRows(); err != nil {
				l.Release()
				return err
			}

			time.Sleep(RetryTime)
		}
	}
	return nil
}

func (l *Lock) lockRow(row interface{}) (applied bool, err error) {
	var prevLockID gocql.UUID
	q := fmt.Sprintf("UPDATE %q.%q USING TTL ? SET %q = ?"+
		" WHERE id = ? IF %q = null",
		l.keyspace, l.table, LockColumn, LockColumn)
	applied, err = l.session.Query(q, int(l.ttl.Seconds()),
		l.lockID, row).ScanCAS(&prevLockID)
	if !applied {
		logf("cannot lock row: lock key=%v lockID=%v prevLockID=%v\n",
			row, l.lockID, prevLockID)
	}
	return
}

func (l *Lock) checkRows() error {
	var countRows int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %q.%q WHERE id in ?",
		l.keyspace, l.table)
	if err := l.session.Query(q, l.rowKeys).Scan(&countRows); err != nil {
		return err
	}
	if countRows != len(l.rowKeys) {
		return errors.New("key not found")
	}
	return nil
}

// Release releases the lock.
func (l *Lock) Release() error {
	for _, k := range l.rowKeys {
		q := fmt.Sprintf("UPDATE %q.%q SET %q = null WHERE id = ? IF %q = ?",
			l.keyspace, l.table, LockColumn, LockColumn)
		if _, err := l.session.Query(q, k, l.lockID).ScanCAS(); err != nil {
			return err
		}
	}
	return nil
}

func logf(format string, args ...interface{}) {
	if Log == nil {
		return
	}
	Log.Printf(format, args...)
}
