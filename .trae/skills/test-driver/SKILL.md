***
name: "test-driver"
description: "Write comprehensive tests following TDD principles for Go projects"
***

# Test Driver Skill

## Purpose
Helps you write comprehensive tests following Test-Driven Development (TDD) principles.

## Test Types

### Unit Tests
- Test individual functions in isolation
- Use table-driven tests
- Mock external dependencies
- Target > 80% coverage

### Integration Tests
- Test interactions between components
- Use test containers when needed
- Cover common workflows

## Go Testing Patterns

### Table-Driven Tests
```go
func TestXxx(t *testing.T) {
    tests := []struct {
        name     string
        input    Input
        expected Output
        wantErr  bool
    }{
        {"valid case", validInput, validOutput, false},
        {"invalid input", badInput, Output{}, true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := Xxx(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("Xxx() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if !reflect.DeepEqual(got, tt.expected) {
                t.Errorf("Xxx() = %v, want %v", got, tt.expected)
            }
        })
    }
}
```

## Best Practices
- Test behavior, not implementation
- Use `t.Parallel()` where appropriate
- Keep tests fast and isolated
- Use descriptive test names
