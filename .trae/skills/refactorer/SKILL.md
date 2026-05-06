***
name: "refactorer"
description: "Safely refactor code with clear steps, tests first, and backward compatibility"
***

# Refactorer Skill

## Purpose
Helps you refactor code safely, with tests first and clear steps.

## Refactoring Checklist
- [ ] Tests pass before starting
- [ ] Small, focused refactors (200 lines)
- [ ] Tests pass after each step
- [ ] No functionality change
- [ ] Backward compatible

## Common Refactorings
1. Extract method
2. Rename variable/function
3. Extract interface
4. Replace conditional with polymorphism
5. Move function
6. Replace magic number with constant

## Go-Specific
- Use `go fmt` / `goimports`
- Keep functions focused
- Prefer composition over inheritance
- Use clear interfaces
- Keep packages focused

## Safety First
Always write/run tests first before refactoring!
