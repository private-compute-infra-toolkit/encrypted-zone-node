# Git Hook for Enforcing Commit Message Standards

The tools/commit_wrapper_hook/ directory provides a `prepare-commit-msg` Git hook designed to
automatically enforce conventional commit message formatting. The script validates the commit
message and performs the following actions:

-   **Aborts the commit** if the subject line exceeds `SUBJECT_CHAR_LIMIT` characters, providing a
    clear error message.
-   **Hard wraps** the message body at `BODY_WRAP_LIMIT` characters, while preserving footers like
    `Bug:` or `Change-Id:`.

## Component Overview

The project is comprised of two primary script files:

1.  **`commit_wrapper.sh`**: This is the core script containing the primary logic for both the
    installation procedure and the message wrapping functionality. Its behavior is conditional,
    determined by the argument it receives (`install` or `hook-impl`).
2.  **`prepare-commit-msg-hook`**: This script serves as the executable hook that is invoked by Git.
    Its sole function is to execute the `commit_wrapper.sh` script with the `hook-impl` argument
    during the appropriate phase of the commit lifecycle.

## Operational Workflow

The operational sequence of the hook is as follows:

1.  The installation command is executed a single time to initialize the hook.
2.  The installation process copies the `prepare-commit-msg-hook` file to the local `.git/hooks/`
    directory, renaming it to `prepare-commit-msg`.
3.  Upon initiating a commit with `git commit`, the Git system automatically executes the hook.
4.  The hook script, in turn, invokes `commit_wrapper.sh hook-impl`, which performs an in-place
    modification of the commit message file.
5.  Subsequently, Git launches the user-configured text editor, presenting the pre-formatted message
    for final review and confirmation.

## Installation Procedure

**Execute the installation command** from the repository's root:

```shell
> chmod +x tools/commit_wrapper_hook/commit_wrapper.sh
> tools/commit_wrapper_hook/commit_wrapper.sh install
```

This command will copy the hook file to the appropriate `.git/hooks/` location and set its
executable permissions. The hook will now automatically format messages for all subsequent
`git commit` operations.

## Integrity of Existing Hooks

The installation script is designed to exclusively create or modify the
`.git/hooks/prepare-commit-msg` file. It is guaranteed not to interfere with, modify, or delete any
other pre-existing Git hooks, such as `pre-commit` or `commit-msg`.

## Usage

Use `git commit -m "<message>"` to commit. The hook will automatically validate the subject line
length and wrap the message body.

> **Note** This hook runs before opening the editor. See
> [this link](https://git-scm.com/docs/githooks#_prepare_commit_msg). So, This hook will not fix
> commit messages that you write on the editor. It is primarily for `git commit -m`. In that case,
> you can config your editor accordingly. For example for nano,
> `git config --global core.editor <editor command>`

## Uninstallation Process

To uninstall the hook, the `prepare-commit-msg` file should be removed from the `.git/hooks`
directory:

```shell
rm .git/hooks/prepare-commit-msg
```
