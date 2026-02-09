# DevKit

## Build

To build the code reproducibly use one of following commands.

Direct build on your machine if you have bazel installed:

```bash
bazel build //...
```

Build in a dedicated container with all the needed tools:

```bash
devkit/build bazel build //...
```

You can also enter the container, and do the build interactivelly:

```bash
devkit/build
> bazel build //...
> exit
```

Each of these commands results in an identical set of cache entries, ensuring cache reuse.

## Dev

The `dev` container inherits from `build` container and few useful tools.

### Pre-Commit

To run `pre-commit` execute one of following:

```bash
devkit/dev
> pre-commit run --all-files
> exit
```

```bash
devkit/dev pre-commit run --all-files
```

```bash
devkit/pre-commit
```

The `pre-commit` can be executed automatically during `git commit`. To enable it, add below script
in `.git/hooks/pre-commit`:

```bash
#!/usr/bin/env bash
devkit/pre-commit
```

### Gitlint

The `gitlint` tool validates conventional commit message format.

To run it on latest commit:

```bash
devkit/gitlint
```

To execute this check during `git commit` add below line in `.git/hooks/commit-msg`:

```bash
devkit/gitlint
```

You can add this as a first step in the script. Be careful to not remove any exeisting code from
there as it implements the commit message hook that add change ID.

### Gemini

To run `gemini` execute one of following:

```bash
devkit/dev
> gemini
> exit
```

```bash
devkit/dev gemini
```

```bash
devkit/gemini
```

Remember the you have to set and export the `GEMINI_API_KEY` in your environment.

## Coverage

The coverage check can be executed using this command:

```bash
devkit/coverage --lines 10
```

This invocatioin will check that all files which are tested by some UT have at least 10% line
coverage.

## IDE

You can run IDEs both on Cloudtop, and remotelly. Similarly to the `dev` container, they inherit
from the `dev` container, so all the tools will be available also in the console opened from any of
those IDEs.

The caches will also be reused in the build executed from the IDEs.

For now the plugins are not installed automatically, so you have to add e.g.: the `rust-analyser` in
VSCode.

### Linux workstation with X11

Those commands will start IDEs with GUI rendered using X11:

```bash
devkit/vscode_ide
```

```bash
devkit/clion_ide
```

### Remote

The VSCode can be also started in server mode, to be used from a browser:

```bash
devkit/vscode_ide --server
```

To be able to open it in a browser on your local machine you need to connect over SSH with proper
port mapping:

```bash
ssh -L 8080:localhost:8080 your_linux_workstation.c.googlers.com
```

Then you can open the IDE in a browser at `http://localhost:8080`.

## Working in subprojects

The instructions above apply also to subprojects. The only difference is that you have to refeer to
DevKit commands from the parent directory. Exmmple:

```bash
cd crypto_oracle/
../devkit/build ls
```
