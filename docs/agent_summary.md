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

The response from the agent should return a JSON blob consistent withour API definition in the
response. If it does not, immediately return an exception and stop the program.

If the JSON response contains data requests, we will launch the retrievers to fetch the data, and
repeat the request with the instructions, the requested data, and the "agent_response" given by
the Agent.

If there are no more data requests, we are done and we give the "agent_response" directly back to 
the user.