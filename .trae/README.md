# Superpowers Skills for Data Ingestion Tool

Welcome to your enhanced development environment! This project is configured with **Superpowers**, a set of skills to help you write better code faster.

## 🚀 Quick Start

### Using Skills
Invoke skills by mentioning their name or purpose, e.g.:
- *"Use superpowers-gstack-hybrid to add X feature"*
- *"Run code-review on pkg/connector"*
- *"Write tests for layered_storage"*

## 📦 Installed Skills

### 1. superpowers-gstack-hybrid (Default)
**Purpose**: Enterprise-grade development workflow for Go projects.
- **When to use**: Starting complex features, refactoring, production code
- **Features**: TDD, multi-role reviews, design docs, performance benchmarks

### 2. code-review
**Purpose**: Comprehensive code reviews
- **When to use**: Before merging PRs, self-reviewing code

### 3. test-driver
**Purpose**: TDD-focused test writing
- **When to use**: Adding tests, TDD workflow

### 4. refactorer
**Purpose**: Safe refactoring
- **When to use**: Restructuring code, improving design

### 5. bug-hunter
**Purpose**: Systematic debugging
- **When to use**: Fixing bugs, troubleshooting issues

### 6. security-auditor
**Purpose**: Security vulnerability checks
- **When to use**: Security reviews, compliance checks

### 7. performance-tuner
**Purpose**: Optimize performance
- **When to use**: Profiling, benchmarking, speed improvements

### 8. doc-writer
**Purpose**: Documentation
- **When to use**: Writing docs, API docs, design records

## 📁 Project Structure

```
.trae/
├── skills/              # All Superpowers skills
│   ├── superpowers-gstack-hybrid/
│   ├── code-review/
│   ├── test-driver/
│   ├── refactorer/
│   ├── bug-hunter/
│   ├── security-auditor/
│   ├── performance-tuner/
│   └── doc-writer/
├── documents/           # Generated docs, audits, reports
└── specs/               # Specifications for features
```

## 📄 Documents Index

### Audit Reports
- [Code Audit Plan](documents/code-audit-plan.md) - 代码审计计划
- [Code Audit Report](documents/code-audit-report.md) - 代码审计报告
- [Execution Plan Audit Report](documents/execution-plan-audit-report.md) - 执行计划审计报告
- [Code Quality Analysis](documents/code_quality_analysis.md) - 代码质量分析
- [Project Cleanup Report](documents/project-cleanup-report.md) - 项目清理报告

### Implementation Plans
- [Configure GOPROXY Plan](documents/configure-goproxy-plan.md) - Go代理配置计划
- [Refactor Layered Storage Plan](documents/refactor-layered-storage-plan.md) - 重构分层存储计划

### Delivery Reports
- [TDD Delivery Report](documents/tdd-delivery-report.md) - 测试驱动开发交付报告
- [Test Coverage Improvement Report](documents/test-coverage-improvement-report.md) - 测试覆盖率提升报告

### Design Docs
- [Enterprise Data Lake Enhancement](documents/enterprise_data_lake_enhancement.md) - 企业数据湖增强方案

## 📋 Specs Index

### High Priority Improvements
- [Spec](specs/high-priority-improvements/spec.md) - 高优先级改进规格
- [Tasks](specs/high-priority-improvements/tasks.md) - 任务列表
- [Checklist](specs/high-priority-improvements/checklist.md) - 检查清单

### Quality Improvement Campaign
- [Spec](specs/quality-improvement-campaign/spec.md) - 质量改进活动规格
- [Tasks](specs/quality-improvement-campaign/tasks.md) - 任务列表
- [Checklist](specs/quality-improvement-campaign/checklist.md) - 检查清单

## 🏆 Workflows for Common Tasks

### Adding a New Feature (Complex - L2/L3)
```
1. Use "superpowers-gstack-hybrid"
2. Write Design Doc
3. Create PLAN.md
4. Write tests first
5. Implement
6. Code review
7. Test & verify
8. Ship!
```

### Fixing a Bug
```
1. Use "bug-hunter"
2. Reproduce issue
3. Write failing test
4. Fix bug
5. Verify all tests pass
6. Add regression test
7. Self-review with "code-review"
```

### Refactoring
```
1. Use "refactorer"
2. All tests passing first
3. Small, focused changes
4. Tests after each step
5. Review with "code-review"
```

### Optimizing Performance
```
1. Use "performance-tuner"
2. Benchmark
3. Profile
4. Optimize
5. Re-benchmark to verify improvement
```

### Code Review
```
1. Use "code-review"
2. Check all categories (functionality, readability, perf, security, testing)
3. Provide actionable feedback
4. Approve or request changes
```

## 🔧 Configuration

Check `trae.json` for project config.

---

Happy coding with Superpowers! 🚀
