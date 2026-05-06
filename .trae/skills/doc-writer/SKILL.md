***
name: "doc-writer"
description: "Write clear, helpful documentation for Go projects"
***

# Doc Writer Skill

## Purpose
Helps write clear, helpful documentation following best practices.

## Documentation Types

### README.md
- Project purpose and overview
- Quickstart guide
- Installation
- Configuration
- Examples

### Go Docs
- Package-level doc for every package
- Exported function/type/method documentation
- Usage examples
- Non-trivial edge cases documented

### Design Docs
Record architectural decisions:
- Context
- Decision
- Rationale
- Alternatives
- Consequences

### API Docs
- REST API endpoints
- Parameters
- Responses
- Examples

## Good Docs Principles
1. Clear, not clever
2. Examples first
3. Code comments explain "why"
4. Keep docs close to code
5. Test examples

## Go Doc Comments
```go
// Package pkg provides data processing functions.
//
// Example:
//
//	result, err := pkg.Process(data)
//	if err != nil {
//	    log.Fatal(err)
//	}
package pkg

// Process transforms input data according to rules.
// It returns ErrInvalidInput if data is invalid.
func Process(data []byte) (Result, error)
```
