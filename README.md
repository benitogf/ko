# ko

[![Test](https://github.com/benitogf/ko/actions/workflows/tests.yml/badge.svg)](https://github.com/benitogf/ko/actions/workflows/tests.yml)

In-memory storage with persistence adapter for the [ooo](https://github.com/benitogf/ooo) ecosystem.

## Features

- **In-memory storage** with optional disk persistence
- **LevelDB backend** for durable data storage
- **Compatible with ooo** server storage interface

## Installation

```bash
go get github.com/benitogf/ko
```

## Usage

```go
package main

import (
    "log"

    "github.com/benitogf/ko"
    "github.com/benitogf/ooo"
)

func main() {
    // Create persistent storage
    store := &ko.Storage{Path: "/data/myapp"}
    err := store.Start([]string{}, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Use with ooo server
    server := ooo.Server{
        Storage: store,
    }
    server.Start("0.0.0.0:8800")
    server.WaitClose()
}
```

## Related Projects

- [ooo](https://github.com/benitogf/ooo) - Main server library
- [ooo-client](https://github.com/benitogf/ooo-client) - JavaScript client
- [auth](https://github.com/benitogf/auth) - JWT authentication
- [mono](https://github.com/benitogf/mono) - Full-stack boilerplate