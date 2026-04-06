# MCP Tools

The `tools/mcp/` directory contains the MCP (Model Context Protocol) tools for the EZ node repo.

## Setup

To build and install the MCP tool, run the following `install` build target. This script will build
the server and place it in `tools/mcp/ez-mcp`.

```sh
devkit/build bazel run //tools/mcp/server:install
```

Then add this section to your Gemini settings JSON file, `settings.json` in the repo's `.gemini`
directory:

```json
  "mcpServers": {
    "ez-mcp": {
      "command": "<ez repo root>/tools/mcp/ez-mcp/ez-mcp",
      "cwd": "<ez repo root>",
      "timeout": 30000
    }
  },
```

## Usage

Launch `gemini` in a terminal and you should now see an additional mcp tool called `ez-mcp` with the
command `ez_build_fix`.

```sh
devkit/gemini
```
