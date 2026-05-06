***
name: "bug-hunter"
description: "Systematically debug, reproduce, and fix bugs"
***

# Bug Hunter Skill

## Purpose
Helps you systematically debug, reproduce, and fix bugs.

## Bug Fix Workflow

### 1. Reproduce
- Write failing test first
- Document steps to reproduce
- Check if it's consistent

### 2. Diagnose
- Read logs
- Add debug logging
- Check edge cases

### 3. Fix
- Smallest possible fix
- Test passes
- Add regression test

### 4. Verify
- All tests pass
- Race detector passes
- Lint passes

## Go Debug Tips
- Use `fmt.Printf` / `log.Printf`
- Use GoLand Debugger
- Use `go test -v`
- Use `pprof` for perf issues
- Use `go doc` for types
