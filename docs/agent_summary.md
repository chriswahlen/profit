# Agent Flow

## Definitions

Agent: This is the LLM, so when we say "The Agent decides X", we mean whatever LLM we call/use.

## Components

### LLM Wrapper (Agent Wrapper)

This python class is a generic wrapper for calling out to Agents. We will start with two
implementations:

- The first is a stub wrapper that will serve back canned data. It'll have a map of text keywords
to canned response that can be set either by dry-run clients, or by unit tests.
- The second is a real wrapper around OpenAI's API. Default to using gpt-5-nano.
  Allow overriding the model name (e.g., gpt-5-nano, gpt-4o) and other call parameters
  (temperature, max tokens, stop sequences, retry policy) through config or explicit kwargs so
  users can tune agent behavior while tests can pin deterministic settings.

### Data Fetching API

We define a JSON API that the Agent can use to tell us we need to give it some raw data. The
specifics are given in `json_fetching_api`.md.

### Retrievers

We have retrievers for each of the data request types. They take a json input and return a formatted
set of results that is concise and ready for the Agent to consume.

## Data Flow

We take an initial prompt from the user, and append this prompt to an initial set of instructions
for the Agent on how to use our API. This initial instruction set lives in a canned response we
store in the repo.

The response from the agent should return a JSON blob consistent with our API definition in the
response. Validate against the JSON schema (canonical IDs, UTC dates, inclusive end date, JSON null
for open bounds, aggregation arrays) and reject on any mismatch; log a concise warning with
provider, symbol/region, and window when rejecting.

If you modify this agent flow (new retriever types, snippet behavior, validation rules, logging
contracts), update `planner.md` too so the live prompt reflects the new requirements.

For invalid or partial replies, log structured details: warn when validation fails, emit INFO
entries for empty/zero-point responses (include provider/exchange and requested window), and
capture structured error codes/messages whenever retrievers cannot fulfill a request. Tie these
logs to the prompt/response snippets (temp files for anything >4 KB) and include the derived
next-step plan (which retrievers/filters to run or whether we're returning directly to the user).

If the agent signals that a new research snippet should be stored (e.g., via a `snippet` request
with `action: "store"`), validate the snippet schema (title, body, normalized tags, related
instruments/regions, timestamps) before persisting. When snippets are retrieved, summarize the
matches in the next prompt and mention their `snippet_id`s so the Agent can reference or build on
them. Log both snippet stores and lookups (with prompt/response persistence if large), including why
each snippet was picked.

If the JSON response contains data requests, we will launch the retrievers to fetch the data, and
repeat the request with the instructions, the requested data, and the "agent_response" given by
the Agent.

Always log the prompt we actually sent to the Agent (initial instructions + user request). If the
prompt exceeds a few kilobytes (≈4 KB), write it to a temporary file, log the file path, and log
only a concise snippet (e.g., first and last 200 characters) in the main log line. Apply the same
guidance to the Agent’s response: log the full text when it’s short, otherwise persist it to a temp
file and reference that path plus a snippet. Include in the log what we understood from the
response (e.g., “invoke `market` retriever for `XNAS|AAPL` with fields open/close, 7d_avg window”,
or “return final response to user”), so that downstream debugging can reconstruct the decision.

If there are no more data requests, we are done and we give the "agent_response" directly back to 
the user.

Add a small guardrail loop: cap iterations to avoid infinite back-and-forth; when the cap is hit,
surface a clear error to the user and include the most recent agent_response for context.

## Prompt usage

Seed each turn with `planner.md` plus user context (and snippet summaries if retrieved). Embed the
planner text verbatim before you ask for new instruction.
