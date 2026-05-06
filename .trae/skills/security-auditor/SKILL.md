***
name: "security-auditor"
description: "Audit Go code for security vulnerabilities"
***

# Security Auditor Skill

## Purpose
Audit Go code for security vulnerabilities and best practices.

## OWASP Top 10 Check
- [ ] Injection (SQL, NoSQL, command)
- [ ] Broken authentication
- [ ] Sensitive data exposure
- [ ] XXE
- [ ] Broken access control
- [ ] Security misconfiguration
- [ ] XSS
- [ ] Insecure deserialization
- [ ] Using known vulnerabilities
- [ ] Insufficient logging

## Go-Specific Checks
- [ ] No hardcoded secrets
- [ ] Input validation
- [ ] Safe SQL queries (no fmt.Sprintf)
- [ ] TLS used
- [ ] Error messages not exposing internals
- [ ] Random numbers using crypto/rand
- [ ] Safe file I/O (no path traversal)
- [ ] Safe HTTP requests (no SSRF)

## Tools
- `gosec ./...
- `nancy` for dependencies
- `golangci-lint`
