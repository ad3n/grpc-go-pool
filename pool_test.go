package grpcpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNew(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 3, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 3 {
		t.Errorf("The pool available was %d but should be 3", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Get a client
	client, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}
	if client == nil {
		t.Error("client was nil")
	}
	if a := p.Available(); a != 2 {
		t.Errorf("The pool available was %d but should be 2", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Return the client
	err = client.Close()
	if err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 3 {
		t.Errorf("The pool available was %d but should be 3", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Attempt to return the client again
	err = client.Close()
	if err != ErrAlreadyClosed {
		t.Errorf("Expected error \"%s\" but got \"%s\"",
			ErrAlreadyClosed.Error(), err.Error())
	}

	// Take 3 clients
	cl1, err1 := p.Get(context.Background())
	cl2, err2 := p.Get(context.Background())
	cl3, err3 := p.Get(context.Background())
	if err1 != nil {
		t.Errorf("Err1 was not nil: %s", err1.Error())
	}
	if err2 != nil {
		t.Errorf("Err2 was not nil: %s", err2.Error())
	}
	if err3 != nil {
		t.Errorf("Err3 was not nil: %s", err3.Error())
	}

	if a := p.Available(); a != 0 {
		t.Errorf("The pool available was %d but should be 0", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Returning all of them
	err1 = cl1.Close()
	if err1 != nil {
		t.Errorf("Close returned an error: %s", err1.Error())
	}
	err2 = cl2.Close()
	if err2 != nil {
		t.Errorf("Close returned an error: %s", err2.Error())
	}
	err3 = cl3.Close()
	if err3 != nil {
		t.Errorf("Close returned an error: %s", err3.Error())
	}
}

func TestCapacity(t *testing.T) {
	// Assign negative capacity, test that it is changed to 1
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, -1, -2, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 1 {
		t.Errorf("The pool available was %d but should be 1", a)
	}
	if a := p.Capacity(); a != 1 {
		t.Errorf("The pool capacity was %d but should be 1", a)
	}

	// Assing more initial connections than capacity, test that it is changed to cap limit
	p, err = New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 100, 2, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 2 {
		t.Errorf("The pool available was %d but should be 2", a)
	}
	if a := p.Capacity(); a != 2 {
		t.Errorf("The pool capacity was %d but should be 2", a)
	}
}

func TestTimeout(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	_, err = p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 0 {
		t.Errorf("The pool available was %d but expected 0", a)
	}

	// We want to fetch a second one, with a timeout. If the timeout was
	// ommitted, the pool would wait indefinitely as it'd wait for another
	// client to get back into the queue
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Millisecond))
	defer cancel()

	_, err2 := p.Get(ctx)
	if err2 != ErrTimeout {
		t.Errorf("Expected error \"%s\" but got \"%s\"", ErrTimeout, err2.Error())
	}
}

func TestMaxLifeDuration(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0, 1)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	c, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}

	// The max life of the connection was very low (1ns), so when we close
	// the connection it should get marked as unhealthy
	if err = c.Close(); err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}
	if !c.unhealthy {
		t.Errorf("the connection should've been marked as unhealthy")
	}

	// Let's also make sure we don't prematurely close the connection
	count := 0
	p, err = New(func() (*grpc.ClientConn, error) {
		count++
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0, time.Minute)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	for i := 0; i < 3; i++ {
		c, err = p.Get(context.Background())
		if err != nil {
			t.Errorf("Get returned an error: %s", err.Error())
		}

		// The max life of the connection is high, so when we close
		// the connection it shouldn't be marked as unhealthy
		if err := c.Close(); err != nil {
			t.Errorf("Close returned an error: %s", err.Error())
		}
		if c.unhealthy {
			t.Errorf("the connection shouldn't have been marked as unhealthy")
		}
	}

	// Count should have been 1 as dial function should only have been called once
	if count > 1 {
		t.Errorf("Dial function has been called multiple times")
	}
}

func TestIdleTimeout(t *testing.T) {
	// Let's test idle timeout for connection laying in the pool
	// Call Get 3 times. 2 of them will happen before idle timeout, one after. Connection should be created 2 times total
	count := 0
	p, err := New(func() (*grpc.ClientConn, error) {
		count++
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, time.Millisecond)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}
	c, _ := p.Get(context.Background())
	c.Close()
	c, _ = p.Get(context.Background())
	c.Close()
	time.Sleep(10 * time.Millisecond)
	c, _ = p.Get(context.Background())
	c.Close()
	if count != 2 {
		t.Errorf("Dial function has been called only %v times, expected 2", count)
	}
}

func TestPoolClose(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	c, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}

	cc := c.ClientConn
	if err = c.Close(); err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}

	// Close pool should close all underlying gRPC client connections
	p.Close()

	if cc.GetState() != connectivity.Shutdown {
		t.Errorf("Returned connection was not closed, underlying connection is not in shutdown state")
	}

	if p.Capacity() != 0 {
		t.Errorf("Closed pool capacity is not zero")
	}

	if p.Available() != 0 {
		t.Errorf("Closed pool availability is not zero")
	}

	_, err = p.Get(context.Background())
	if err != ErrClosed {
		t.Errorf("Closed pool returned unexpected error")
	}
}

func TestContextCancelation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewWithContext(ctx, func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		default:
			return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

	}, 1, 1, 0)

	if err != context.Canceled {
		t.Errorf("Returned error was not context.Canceled, but the context did cancel before the invocation")
	}
}
func TestContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	_, err := NewWithContext(ctx, func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		// wait for the deadline to pass
		case <-time.After(time.Millisecond):
			return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

	}, 1, 1, 0)

	if err != context.DeadlineExceeded {
		t.Errorf("Returned error was not context.DeadlineExceeded, but the context was timed out before the initialization")
	}
}

func TestGetContextTimeout(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0)

	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	// keep busy the available conn
	_, _ = p.Get(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	// wait for the deadline to pass
	time.Sleep(time.Millisecond)
	_, err = p.Get(ctx)
	if err != ErrTimeout { // it should be context.DeadlineExceeded
		t.Errorf("Returned error was not ErrTimeout, but the context was timed out before the Get invocation")
	}
}

func TestGetContextFactoryTimeout(t *testing.T) {
	p, err := NewWithContext(context.Background(), func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		// wait for the deadline to pass
		case <-time.After(100 * time.Millisecond):
			return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

	}, 1, 1, 0)

	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	// mark as unhealty the available conn
	c, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}
	c.Unhealthy()
	c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err = p.Get(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("Returned error was not context.DeadlineExceeded, but the context was timed out before the Get invocation")
	}
}

func TestNilPtr(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0)

	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	c, _ := p.Get(context.Background())
	c.Unhealthy() // suppress linter warning before assigning to nil

	// after setting pointers to nil functions should return, no crash should be triggered
	p = nil
	p.Get(context.Background())
	p.getClients()
	p.put(nil)
	p.IsClosed()
	p.Available()
	p.Capacity()
	p.Close()

	c = nil
	c.Unhealthy()
	c.Close()
}

// tun Racing tests with `-race` flag
func TestConnRacing(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, 0)

	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	var wg sync.WaitGroup
	const iterations = 10

	// Attempt to close same connection in different threads
	conn, _ := p.Get(context.Background())
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go conn.Close()
		wg.Done()
	}
	wg.Wait()

	// Check racing condintion with Pool and Connection close
	conn, _ = p.Get(context.Background())
	go p.Close()
	go conn.Close()
}

func TestPoolRacing(t *testing.T) {
	var wg sync.WaitGroup
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		p, err := New(func() (*grpc.ClientConn, error) {
			return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
		}, 1, 1, 0)

		if err != nil {
			t.Errorf("The pool returned an error: %s", err.Error())
		}

		// deliberately call multiple Close commands to force race
		conn, _ := p.Get(context.Background())
		go p.Close()
		go p.Close()
		go conn.Close()
		go conn.Close()

		wg.Done()
	}
	wg.Wait()
}
