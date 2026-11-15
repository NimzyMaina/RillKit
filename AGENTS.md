# AGENTS.md — Repository & Agent Operational Standards

## 0. Project Guide (READ FIRST)
- **Read and follow [CONTRIBUTING.md](CONTRIBUTING.md) before making any change.** It describes this repository (the shared `spec/`, the SDKs under `sdks/`, the docs), the rules that keep the SDKs compatible, the exact commands to verify a change, and step-by-step recipes for common changes.
- These standards and CONTRIBUTING.md apply together. Where they overlap, follow the stricter rule.
- `CLAUDE.md` and `GEMINI.md` are symlinks to this file, so every AI tool reads the same rules. Edit `AGENTS.md` only.

## 1. Git & Version Control (STRICT — NO EXCEPTIONS)
- **Branch Protection:** NEVER commit directly to `main`, `master`, `develop`, or `production`. Before editing any files, run `git branch --show-current`. If on a protected branch, create and switch to a branch first using `feat/<kebab-case>`, `fix/<kebab-case>`, or `chore/<kebab-case>`.
- **Commit Identity:** NEVER modify `git config user.name` or `git config user.email`, and NEVER pass `--author` flags impersonating an AI (e.g., "Claude", "Gemini", "Copilot", "Cursor"). All commits must use the local machine's existing human Git identity.
- **Zero AI Attribution:** Do NOT include `Co-authored-by:`, `Generated with`, or any AI tool signatures in commit messages, PR bodies, or code comments.
- **Mandatory Commit & Push Approval (STRICT):** NEVER execute `git commit`, `git push`, `git merge`, or `gh pr create` without explicit prior approval from the user.
  - **Required Workflow:**
    1. Run the linter and test suite to verify changes, then stage your files (`git add <files>`) and run `git status` and `git diff --staged`.
    2. Stop and display a summary in chat containing:
       - **Target Branch:** (confirming it is a feature branch, not `main`/`master`)
       - **Staged Files:** (list of modified/added/deleted files)
       - **Proposed Commit Message:** (formatted cleanly with zero AI attribution)
    3. Ask: *"Do you approve this commit?"* and wait for an explicit affirmative reply (e.g., "yes", "approved", "commit") before running `git commit`.
  - **Multi-Commit Tasks:** Even if a task spans multiple logical commits, pause and request approval before *each* individual commit.
- **Atomic Commits:** Keep commits focused on a single logical change. Do not bundle unrelated formatting changes or refactoring with bug fixes or features.
- **Destructive Git Commands Forbidden:** NEVER run `git push --force`, `git reset --hard`, `git clean -fd`, or amend commits that have already been pushed unless explicitly instructed by the user.

## 2. Verification & Definition of Done
- **Verify Before Finishing:** Never consider a task complete until you have run the project's test suite, linter, and type-checker and confirmed zero new errors.
- **Fix Root Causes, Not Symptoms:** Do not silence compiler/linter warnings with `@ts-ignore`, `# type: ignore`, `eslint-disable`, or empty `catch` blocks to make checks pass.
- **Write Tests for Behavior Changes:** Any new feature or bug fix must include corresponding unit or integration tests covering both the happy path and edge cases.

## 3. Scope & Code Hygiene
- **Stay in Scope:** Only modify files directly relevant to the requested task. Avoid drive-by refactoring, reformatting untouched files, or renaming public APIs unless asked.
- **Match Existing Patterns:** Follow the codebase's established conventions for directory structure, naming, error handling, and dependencies before introducing new libraries.
- **Clean Up Artifacts:** Remove any temporary scripts, debug `console.log`/`print` statements, scratch files, or `.bak` files before finishing.

## 4. Security & Environment Safety
- **Protect Secrets:** NEVER read, print, log, or commit `.env` files, API keys, tokens, credentials, or private keys (`*.pem`, `*.key`). If a secret is accidentally staged, stop immediately and alert the user.
- **No Unapproved Dependencies:** Do not install new packages (`npm install`, `pip install`, `cargo add`, etc.) without checking if an existing dependency already solves the problem or asking first.
- **Destructive System Actions:** Never drop database tables, delete migrations, or modify CI/CD deployment workflows without explicit confirmation.

## 5. Multi-Agent & Handoff Hygiene
- **Respect Uncommitted Work:** Before starting, run `git status`. If there are uncommitted changes you did not create, do NOT overwrite or revert them—ask the user how to proceed.
- **Leave a Clear Summary:** When finishing a task, briefly list: (1) files modified, (2) commands/tests run to verify, and (3) any follow-up actions or migrations required.

## 6. Sub-Agent Delegation & Parallel Execution
- **Delegate for Speed:** If your environment supports spawning sub-agents or parallel background tasks, proactively delegate independent workstreams whenever doing so reduces total delivery time.
- **High-Value Delegation Candidates:**
  - **Codebase Exploration:** Spawn parallel read-only sub-agents to search files, trace dependencies, or gather context across separate modules simultaneously.
  - **Independent Modules:** Split implementation across sub-agents when changes touch decoupled files with clear interfaces (e.g., frontend UI vs. backend API, or implementation vs. test suite generation).
  - **Parallel Verification:** Run test suites, linters, and type-checks concurrently rather than sequentially where supported.
- **Prevent File & Git Collisions:**
  - **Never Delegate Git Commits:** Sub-agents must NEVER create branches, stage files, or run `git commit`. Only the primary orchestrator agent may perform Git operations on the shared feature branch.
  - **Strict File Ownership:** Assign explicit, non-overlapping file paths to each sub-agent so two agents never edit the same file concurrently.
- **Keep Tight Tasks Local:** Do not spawn sub-agents for trivial single-file edits or sequential tasks where orchestration overhead exceeds execution time.