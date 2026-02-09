# Instructions to setup developer workflow

[go/ps-ez-dev-workflow](http://go/ps-ez-dev-workflow)

## Enable rust analyzer

Note: If rust-analyzer errors when you first open with `rust-analyzer failed to fetch workspace`,
wait for the rust-project.json to be generated in the project root, then restart the extension. This
should only happen the very first time you open the folder when bazel has to create a new
rust-analyzer.json via our .vscode/tasks.json
