# DBOS Transact Go POC

This repository contains a minimal proof-of-concept layout for experimenting with [DBOS Transact](https://github.com/dbos-inc/dbos-transact-golang) in Go. The focus is on scaffolding only—no business logic or workflows are implemented yet.

## Prerequisites
- Go 1.24+
- Access to a PostgreSQL instance (optional for scaffolding; required to run DBOS)

## Getting Started
1. Copy `.env.example` to `.env` and update the values for your environment.
2. Install dependencies:

   ```bash
   go mod tidy
   ```

3. Run the HTTP server (no routes beyond `/healthz` are implemented yet):

   ```bash
   go run ./...
   ```

## Project Structure

```
.
├── internal/
│   ├── api/              # HTTP router and handler skeletons
│   ├── config/           # Viper-based configuration loader
│   ├── db/               # Database initialization helpers and migrations
│   ├── logger/           # Zap logger setup
│   └── workflows/        # DBOS initialization and registration stubs
└── main.go               # Entry point wiring config, logging, DBOS, and router
```

Add services, middleware, workflows, and steps as the project evolves.

