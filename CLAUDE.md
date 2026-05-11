<!-- driverize:v__DRIVERIZE_VERSION__ -->
## Codebase Intelligence — Driver MCP

**Start with Driver tools** for codebase exploration. After loading Driver context (calling any Driver MCP tool), native tools (Grep, Glob, Bash search, Explore/Plan agents) are available for follow-up.

### Decision Tree

| I need to… | Use this Driver tool |
|------------|---------------------|
| Understand a task, get suggested approach | `gather_task_context` (start here) |
| See system architecture | `get_architecture_overview` |
| Navigate directory structure | `get_code_map` |
| Get symbol-level detail for a file | `get_file_documentation` |
| Read actual source code | `get_source_file` or native `Read` |
| See recent changes | `get_changelog` |
| Learn conventions, onboarding tips | `get_llm_onboarding_guide` |

### After Driver Returns

Use native tools (Read, Edit, Write, Grep, Glob, Bash, Explore/Plan agents) for follow-up work. Driver gives you the map; native tools do the surgery.

### Examples

**"Add retry logic to the API client"**
→ `gather_task_context` with description: "Add retry logic to the API client. Need to find the HTTP client implementation, understand error handling patterns, and identify where retries should be added."

**"Where is the authentication middleware defined?"**
→ `get_code_map` at the top level, then `get_file_documentation` on the auth-related file Driver identifies.

**"How does the billing service calculate invoices?"**
→ `gather_task_context` with description: "Trace the invoice calculation flow in the billing service. Need to understand the data model, calculation logic, and integration points."

**"Refactor the logger to use structured output"**
→ `gather_task_context` first, then `Read` the specific files, then `Edit` to make changes.
<!-- /driverize -->
