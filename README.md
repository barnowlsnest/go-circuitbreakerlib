# go-circuitbreakerlib

[![Build and Test](https://github.com/barnowlsnest/go-circuitbreakerlib/actions/workflows/build.yml/badge.svg)](https://github.com/barnowlsnest/go-circuitbreakerlib/actions/workflows/build.yml)
[![Lint](https://github.com/barnowlsnest/go-circuitbreakerlib/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/barnowlsnest/go-circuitbreakerlib/actions/workflows/golangci-lint.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/barnowlsnest/go-circuitbreakerlib.svg)](https://pkg.go.dev/github.com/barnowlsnest/go-circuitbreakerlib)
[![Go Report Card](https://goreportcard.com/badge/github.com/barnowlsnest/go-circuitbreakerlib)](https://goreportcard.com/report/github.com/barnowlsnest/go-circuitbreakerlib)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A production-grade circuit breaker for Go services that call remote
dependencies over HTTP, gRPC, or TCP. A dependency-free core state machine
with sliding windows, stackable trip policies, bounded half-open probing,
and epoch-checked permits; a keyed registry; nestable composite breakers
per dependency and per instance; thin protocol adapters; and hooks for
logging and telemetry.

**Status:** v0. The exported API may change between minor versions until
the adapters have run in a real gateway.

## Installation

```bash
go get github.com/barnowlsnest/go-circuitbreakerlib
```

Requires Go 1.27 or later.

## Quick start

_Coming with the adapters._

## Core

_Coming with `pkg/breaker`._

## Hooks

_Coming with `pkg/hooks`._

## Registry

_Coming with `pkg/registry`._

## Composite

_Coming with `pkg/composite`._

## Adapters

_Coming with `pkg/adapter`._

## Benchmarks

_Coming with the docs PR._
