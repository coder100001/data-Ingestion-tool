***
name: "performance-tuner"
description: "Optimize Go code with profiling and benchmarking"
***

# Performance Tuner Skill

## Purpose
Helps optimize Go code with profiling and benchmarking.

## Benchmarking
```go
func BenchmarkXxx(b *testing.B) {
    for i := 0; i < b.N; i++ {
        Xxx()
    }
}
```

## Profiling
- CPU: `go test -cpuprofile=cpu.prof`
- Memory: `go test -memprofile=mem.prof`
- Trace: `go test -trace=trace.out`
- Block: `go test -blockprofile=block.prof`

## Go Performance Tips
- Avoid unnecessary allocations
- Pre-allocate slices with known capacity
- Use sync.Pool for frequent objects
- Minimize cgo calls
- Use efficient algorithms
- Leverage concurrency where appropriate
- Use buffers instead of strings (string concatenation)
