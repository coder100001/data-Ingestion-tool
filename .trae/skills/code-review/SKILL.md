***
name: "code-review"
description: "Perform comprehensive code reviews with quality gates and actionable feedback"
***

# Code Review Skill

## Purpose
This skill helps you review code with quality gates, best practice checks, and actionable suggestions.

## Review Categories

### 1. Functionality
- [ ] Does the code work as intended?
- [ ] Are edge cases handled?
- [ ] Is error handling robust?

### 2. Readability & Maintainability
- [ ] Are names descriptive?
- [ ] Is there appropriate documentation/comments?
- [ ] Are functions kept short (< 50 lines)?

### 3. Performance
- [ ] Any obvious performance issues?
- [ ] Are resources properly released?
- [ ] Any unnecessary computations?

### 4. Security
- [ ] Any injection vulnerabilities?
- [ ] Sensitive data properly handled?
- [ ] Input validation present?

### 5. Testing
- [ ] Unit tests covering main paths?
- [ ] Error scenarios tested?
- [ ] Coverage adequate?

## Output Format
Provide review in markdown with:
1. Summary (overall judgment)
2. Key findings (with severity: Critical/High/Medium/Low)
3. Actionable suggestions
4. Approval status
