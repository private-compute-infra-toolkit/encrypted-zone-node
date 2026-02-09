# MCP Tools

The tools/mcp/ directory contains the MCP (Model Context Protocol) tools for the ez repo.

## Setup

To build the MCP tool, run the `pkg-mcp` build rule. This script will build the server and place it
in `tools/mcp/ez-mcp`.

```bash
bazel run //tools/mcp/server:pkg-mcp
```

Then add the below section to your gemini settings, usually `~/.gemini/settings.json`

```json
  "mcpServers": {
    "ez-mcp": {
      "command": "<ez repo root>/tools/mcp/ez-mcp/server",
      "cwd": "<ez repo root>",
      "timeout": 30000
    }
  },
```

## Usage

Launch `gemini` in a terminal and you should now see an additional mcp tool called `ez-mcp` with a
`ez_build_fix` command.

```bash
gemini
```

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information. t
