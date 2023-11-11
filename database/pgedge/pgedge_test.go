package pgedge

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	// "strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"

	_ "github.com/lib/pq"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 5433

var (
	opts = dktest.Options{
		Cmd:          []string{"bin/yugabyted", "start", "--daemon=false"},
		PortRequired: true,
		ReadyFunc:    isReady,
		Timeout:      time.Duration(60) * time.Second,
	}
	// Released versions: https://docs.yugabyte.com/latest/releases/#current-supported-releases
	specs = []dktesting.ContainerSpec{
		{ImageName: "yugabytedb/yugabyte:2.6.16.0-b14", Options: opts},
		{ImageName: "yugabytedb/yugabyte:2.8.4.0-b30", Options: opts},
		{ImageName: "yugabytedb/yugabyte:2.12.2.0-b58", Options: opts},
		{ImageName: "yugabytedb/yugabyte:2.13.0.1-b2", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		log.Println("port error:", err)
		return false
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://yugabyte:yugabyte@%v:%v?sslmode=disable", ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	if err := db.PingContext(ctx); err != nil {
		log.Println("ping error:", err)
		return false
	}
	if err := db.Close(); err != nil {
		log.Println("close error:", err)
	}
	return true
}

func createDB(t *testing.T, c dktest.ContainerInfo) {
	// ip, port, err := c.Port(defaultPort)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	//replace
	// db, err := sql.Open("postgres", fmt.Sprintf("postgres://yugabyte:yugabyte@%v:%v?sslmode=disable", ip, port))
	db, err := sql.Open("postgres", "postgresql://admin:Z00Wahk3M91ET63k3D2Wx6sP@eagerly-communal-goshawk-dsm.pgedge.io/golang_migrate_test_1?sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	// if _, err = db.Exec("CREATE DATABASE user1s"); err != nil {
	// 	t.Fatal(err)
	// }
}

func getConnectionString(ip, port string, options ...string) string {
	options = append(options, "sslmode=verify-full")

	// replace
	// return fmt.Sprintf("yugabyte://yugabyte:yugabyte@%v:%v/migrate?%s", ip, port, strings.Join(options, "&"))
	return "postgresql://admin:Z00Wahk3M91ET63k3D2Wx6sP@eagerly-communal-goshawk-dsm.pgedge.io/golang_migrate_test_1?sslmode=verify-full"
}

// func Test(t *testing.T) {
// 	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
// 		createDB(t, ci)

// 		ip, port, err := ci.Port(defaultPort)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		addr := getConnectionString(ip, port)
// 		c := &PgEdge{}
// 		d, err := c.Open(addr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		dt.Test(t, d, []byte("SELECT 1"))
// 	})
// }

func TestMigrate(t *testing.T) {
	// replace
	// dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		// createDB(t, ci)
		createDB(t, dktest.ContainerInfo{})
		fmt.Println("im here")

		// ip, port, err := ci.Port(defaultPort)
		// if err != nil {
		// 	t.Fatal(err)
		// }

		// addr := getConnectionString(ip, port)
		addr := getConnectionString("", "")
		c := &PgEdge{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		fmt.Println("im here1", d)

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "postgres", d)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("im here2")
		dt.TestMigrate(t, m)
		fmt.Println("im here3")

	// })
}

// func TestMultiStatement(t *testing.T) {
// 	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
// 		createDB(t, ci)

// 		ip, port, err := ci.Port(defaultPort)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		addr := getConnectionString(ip, port)
// 		c := &PgEdge{}
// 		d, err := c.Open(addr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
// 			t.Fatalf("expected err to be nil, got %v", err)
// 		}

// 		// make sure second table exists
// 		var exists bool
// 		if err := d.(*PgEdge).db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
// 			t.Fatal(err)
// 		}
// 		if !exists {
// 			t.Fatal("expected table bar to exist")
// 		}
// 	})
// }

// func TestFilterCustomQuery(t *testing.T) {
// 	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
// 		createDB(t, ci)

// 		ip, port, err := ci.Port(defaultPort)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		addr := getConnectionString(ip, port, "x-custom=foobar")
// 		c := &PgEdge{}
// 		d, err := c.Open(addr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		dt.Test(t, d, []byte("SELECT 1"))
// 	})
// }
