# Superpowers Skills for Data Ingestion Tool

Welcome to your enhanced development environment! This project is configured with **Superpowers**, a set of skills to help you write better code faster.

## 🚀 Quick Start

### Using Skills
Invoke skills by mentioning their name or purpose, e.g.:
- *"Use superpowers-gstack-hybrid to add X feature"*
- *"Run code-review on pkg/connector"*
- *"Write tests for layered_storage"*

## 📦 Installed Skills (22 Total)

### Core Workflow Skills (from obra/superpowers)

| Skill | Purpose | When to Use |
|-------|---------|-------------|
| **brainstorming** | Socratic design refinement | Before any code, refine rough ideas |
| **writing-plans** | Detailed implementation plans | Breaking down features into tasks |
| **executing-plans** | Batch execution with checkpoints | Running planned tasks |
| **subagent-driven-development** | Fresh subagent per task | Complex multi-task features |
| **dispatching-parallel-agents** | Concurrent subagent workflows | Independent parallel tasks |
| **test-driven-development** | Strict RED-GREEN-REFACTOR | All production code |
| **systematic-debugging** | 4-phase root cause process | Finding and fixing bugs |
| **verification-before-completion** | Prove it's actually fixed | Before declaring done |
| **using-git-worktrees** | Parallel development branches | Isolated feature development |
| **requesting-code-review** | Pre-review checklist | Before merge/PR |
| **receiving-code-review** | Responding to feedback | After code review |
| **finishing-a-development-branch** | Merge/PR decision workflow | Completing a branch |
| **using-superpowers** | Introduction to skills system | Learning the framework |
| **writing-skills** | Create new skills | Extending the framework |

### Custom Skills (Project-Specific)

| Skill | Purpose | When to Use |
|-------|---------|-------------|
| **superpowers-gstack-hybrid** | Enterprise-grade workflow for Go | Complex features, refactoring |
| **code-review** | Comprehensive code reviews | Before merging PRs |
| **test-driver** | TDD-focused test writing | Adding tests |
| **refactorer** | Safe refactoring | Restructuring code |
| **bug-hunter** | Systematic debugging | Fixing bugs |
| **security-auditor** | Security vulnerability checks | Security reviews |
| **performance-tuner** | Optimize performance | Profiling, benchmarking |
| **doc-writer** | Documentation | Writing docs, API docs |

## 📁 Project Structure

```
.trae/
├── skills/              # All Superpowers skills (22 total)
│   ├── brainstorming/
│   ├── writing-plans/
│   ├── executing-plans/
│   ├── subagent-driven-development/
│   ├── test-driven-development/
│   ├── systematic-debugging/
│   ├── verification-before-completion/
│   ├── using-git-worktrees/
│   ├── requesting-code-review/
│   ├── receiving-code-review/
│   ├── finishing-a-development-branch/
│   ├── using-superpowers/
│   ├── writing-skills/
│   ├── dispatching-parallel-agents/
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
1. Use "brainstorming" to refine the idea
2. Use "writing-plans" to create detailed plan
3. Use "using-git-worktrees" for isolation
4. Use "subagent-driven-development" for implementation
5. Use "requesting-code-review" for review
6. Use "finishing-a-development-branch" to merge
```

### Fixing a Bug
```
1. Use "systematic-debugging" to find root cause
2. Use "test-driven-development" to write failing test
3. Fix the bug
4. Use "verification-before-completion" to prove fix
5. Use "code-review" for self-review
```

### Refactoring
```
1. Use "refactorer" for safe refactoring
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
1. Use "requesting-code-review" for pre-checklist
2. Use "code-review" for comprehensive review
3. Use "receiving-code-review" to respond to feedback
```

## 🔧 Configuration

Check `trae.json` for project config.

## 📚 Resources

- [Superpowers Official Repo](https://github.com/obra/superpowers) - 57k+ stars
- [Superpowers Skills Repo](https://github.com/obra/superpowers-skills) - Official skills library

---

Happy coding with Superpowers! 🚀
