---
name: commit_creation
description: >-
    Guides the creation of git commits in the EZ Node repository, enforcing formatting, pre-commit
    hooks, and integration testing.
---

# Commit Creation Skill for EZ Node

When preparing to create a git commit in this repository, follow these strict guidelines:

## 1. Run Pre-Commit Hooks & Environment Preparation

Before attempting to commit, ensure the workspace environment is clean and properly configured:

-   **Check Ignored Files**: If you are creating or modifying workspace-specific agent skills (like
    this one), ensure that `.agents/` is **not** ignored in `.gitignore`. If it is present in
    `.gitignore`, remove it so agent customizations can be successfully committed and tracked.
-   **Run Linters**: Run `devkit/pre-commit run --all-files` (or scoped to staged files if
    appropriate).
-   Address any formatting issues in Rust, C/C++, Bazel, Protobuf, or Markdown before proceeding.

## 2. Format the Commit Message

The commit message must follow the
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/#summary) specification and
match the project requirements:

-   **Format**:

    ```text
    <type>(<scope>): <subject>

    <body>

    Bug: b/<bug-identifier>
    ```

    _(Use `Bug: N/A` if there is no associated Buganizer ID)._

-   **Types**: Typically `feat`, `chore`, `fix`, `docs`, `perf`, `refactor`, or `test`.
-   **Allowed Scopes for `feat` / `fix` / `perf`**: If using a scope, it MUST be one of the
    following (defined in `.versionrc.json`):

    -   `deps`
    -   `diagnostics`
    -   `enforcer`
    -   `ez-to-ez`
    -   `gemini`
    -   `github`
    -   `vscode`
    -   `mtls` _(If none apply, you may omit the scope: `<type>: <subject>`)_

-   **Limits**:

    -   Subject line: Maximum **72 characters**.
    -   Body lines: Wrapped at **80 characters** (excluding special footers like `Bug:` or
        `Change-Id:`). While 80 is the maximum limit, wrapping tighter to **72 characters** is
        highly recommended to avoid warnings from Gerrit's push check.
    -   Use `tools/commit_wrapper_hook/commit_wrapper.sh hook-impl <msg_file>` if committing
        programmatically to help format.

-   **Mandatory Change-Id**: Every commit destined for Gerrit **must** include a `Change-Id` footer.
    To automate this, ensure the Gerrit `commit-msg` hook is installed in your workspace:

    ```bash
    curl -Lo .git/hooks/commit-msg https://gerrit-review.googlesource.com/tools/hooks/commit-msg && chmod +x .git/hooks/commit-msg
    ```

    Additionally, ensure the project's commit wrapping hook is active:

    ```bash
    tools/commit_wrapper_hook/commit_wrapper.sh install
    ```

-   **User Alignment**: **CRITICAL**: If the Bug ID, type, or scope are not provided or are
    ambiguous, you **MUST** ask the user for clarification before suggesting or finalizing the
    commit message.

## 3. Run Integration Tests (Optional but Recommended)

If the changes affect container execution or low-level enforcer logic:

-   Ensure the `devkit` submodule is initialized and updated to the latest remote version:

    ```bash
    git submodule update --init --remote --force
    ```

-   Run the primary integration test using the `devkit` wrapper:

    ```bash
    devkit/build bazel run //enforcer/integration_test:container_integration_test
    ```

## 4. Revising Commits

If you are instructed to revise an existing commit:

-   Always use `git commit --amend` to apply the changes.
-   Ensure you preserve the existing `Change-Id` in the commit message. Do not let it be regenerated
    or lost.

## 5. Pushing Changes

When pushing changes to the remote repository for code review (Gerrit):

-   **Standard Review Push**: If you are targeting `main` (i.e., pushing your changes based on
    `origin/main`), push to the Gerrit review ref:

    ```bash
    git push origin HEAD:refs/for/main
    ```

-   **Work-in-Progress (WIP) Push**: If you want to push a draft or WIP change without notifying
    reviewers or triggering presubmit jobs, append `%wip` to the push ref:

    ```bash
    git push origin HEAD:refs/for/main%wip
    ```

-   **Transitioning WIP to Ready**: Once your WIP change is ready for review, you can either click
    the **"Start Review"** button (located at the top-right of the change details page) in the
    Gerrit UI, or push again with the `%ready` flag:

    ```bash
    git push origin HEAD:refs/for/main%ready
    ```

## 6. Adding Reviewers (Approvers)

After successfully pushing a new CL, you should automatically assign reviewers using the Gerrit REST
API.

-   **Select the Right Reviewer**:
    -   For **tooling, configuration, or agent customization changes** (such as updating agent
        skills or rules): Add **Peter Meric** (`pmeric@google.com`) as the preferred reviewer.
    -   For **core enforcer, isolate, or container changes**: Add **Manish Berwani**
        (`berwani@google.com`) as the preferred reviewer.
-   **Add Reviewer via REST API**: Use `gob-curl` to automatically add them using the Gerrit
    reviewers endpoint:

    ```bash
    gob-curl -X POST -H "Content-Type: application/json" \
      -d '{"reviewer": "<reviewer-email>"}' \
      "https://privacysandbox-review.git.corp.google.com/a/changes/<CL_NUMBER>/reviewers"
    ```

## 7. Monitoring and Auto-Fixing Presubmit Failures (Watch & Fix)

Once you have pushed the CL to Gerrit, actively monitor the status of the presubmits (e.g., Kokoro,
Louhi) and reviewer comments:

1.  **Schedule Polling**: Set up a periodic background timer or cron task to check Gerrit for
    details (using `gob-curl` queries).
2.  **Diagnose Failures**:
    -   If a presubmit check (like `pre-commit`) fails, do **not** rely solely on external web pages
        (like Sponge) if they require corporate SSO.
    -   Instead, immediately attempt to reproduce the check locally using the repository's devkit
        wrappers (e.g., `devkit/pre-commit run --all-files`).
3.  **Apply Auto-Fixes**:
    -   If the local linter run modifies files (e.g., formatting corrections), review the
        `git diff`.
    -   Stage the formatted changes: `git add <files>`.
    -   Amend the commit preserving the `Change-Id`: `git commit --amend --no-edit`.
    -   Re-push to Gerrit: `git push origin HEAD:refs/for/main`.
4.  **Escalate if Uncertain**: If the failure is a compilation error, logic bug, or design issue
    that you cannot fix automatically, immediately escalate to the user with detailed logs.
