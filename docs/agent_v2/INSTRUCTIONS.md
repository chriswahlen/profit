INSTRUCTIONS:

In a new folder called agent_v2, implement an agent state machine with the phases listed in PHASES.
Use the agentiapi library, particularly IMPLEMENTORS.md for hints, as well as the examples.
For each stage, create a python file.

The machine state should be run with the sqlite backend.

Create a command line utility to invoke the agent. The agent takes in a user question, and has
the following parameters:

  --live: use the real openai api
  --retry: retries the last failed step of the previous run.

For each stage:
- Create an individual python file, placed in agent_v2/stages
- Create a unit test for it; in tests/agent_v2/stages
- Define the PROMPT at the top of the file as a multi-line string

PHASES

# Prompt Stage [initial_prompt]

We take the user prompt and instruct the Agent that it is an expert in finance and economics.
We instruct the Agent that it can query our known data sources (Market, SEC/Edgar, Real Estate).
It's job is to generate "insights" it can use, along with the data, to answer the user's prompt.
The first step is to look up any prior insights that might be useful. Insights are tagged, and
also have start/end dates associated with them.

Next stage: [query_prior_insights]

# Query Prior Insights [query_prior_insights]

We'll look up any insights that match the tags and return them, along with the original user prompt.
If there were no matching insights, we'll let the Agent know. At this point, we'll ask the Agent for
the data it would like to query, or if there any other additional insights we should look up. The
requests at this point are still somewhat vague (e.g. "Google Stock Price closing averages in
2024"). Each request is associated with a unique key, which contains the request for data it wants,
and some context as to why it wants the data.

Next Stage: [data_lookup_market, data_lookup_real_estate, data_lookup_sec] => [compile_data]

# DataLookup: Market Data Requests [data_lookup_market]

The agent is requesting data from stock markets, crypto markets, commodity prices, etc. We store
these in our columnar database, so we'll have some canned ways to query this data (daily values,
7-day average, monthly average, etc.). See `schema.md` for how to deliver the details.

Next Stage: [compile_data]

# DataLookup: Real Estate Data [data_lookup_real_estate]

We'll expose the fields we have in our database and allow the AI to construct a query for the data
it will need. The Agent will be able to make repeated queries as necessary. See `schema.md` for how to deliver the details.

Next Stage: [compile_data]

# DataLookup: SEC / Edgar data [data_lookup_sec]

We'll expose the fields we have in our database and allow the AI to construct a query for the data
it will need. The Agent will be able to make repeated queries as necessary (e.g. to query the
available fields - for example, querying the keys a company used to store its capex values, etc).
See `schema.md` for how to deliver the details.

Next Stage: [compile_data]

# Compile Data stage [compile_data]

We report the data back to the Agent all at once. The agent can respond in one of two ways:

1) It can ask for even more data. It can also store any "insights" based on the data it has so far.
Each "insight" has tags associated with it so it can be looked up later, and may optionally have
begin/end dates. If an insight is enough to replace the need for a data set, the Agent can instruct
us to drop the dataset from future context. We then return to [query_prior_insights]

2) It can return a final response to the user. It can also store "insights" just like above.
