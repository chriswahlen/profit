# Step 1: Prompt Phase

We take the user prompt and instruct the Agent that it is an expert in finance and economics.
We instruct the Agent that it can query our known data sources (Market, SEC/Edgar, Real Estate).
It's job is to generate "insights" it can use, along with the data, to answer the user's prompt.
The first step is to look up any prior insights that might be useful. Insights are tagged, and
also have start/end dates associated with them.

# Step 2: Query Prior Insights

We'll look up any insights that match the tags and return them, along with the original user prompt.
If there were no matching insights, we'll let the Agent know. At this point, we'll ask the Agent for
the data it would like to query, or if there any other additional insights we should look up. The
requests at this point are still somewhat vague (e.g. "Google Stock Price closing averages in
2024"). Each request is associated with a unique key, which contains the request for data it wants,
and some context as to why it wants the data.

# Step 3. Data Look Up

The response will fan out at this point based on the data request type. The output of this step
is all of the data requests with their key and the original context associated with them. If no data
was able to be found, the result should say so. All of the results are returned in as compact a
data format as possible.

## Step 3a. Market Data Requests.

The agent is requesting data from stock markets, crypto markets, commodity prices, etc. We store
these in our columnar database, so we'll have some canned ways to query this data (daily values,
7-day average, monthly average, etc.).

## Step 3b. Real Estate Data

We'll expose the fields we have in our database and allow the AI to construct a query for the data
it will need. The Agent will be able to make repeated queries as necessary.

## Step 3c. SEC / Edgar data

We'll expose the fields we have in our database and allow the AI to construct a query for the data
it will need. The Agent will be able to make repeated queries as necessary (e.g. to query the
available fields - for example, querying the keys a company used to store its capex values, etc).

# Step 4

We report the data back to the Agent all at once. The agent can respond in one of two ways:

1) It can ask for even more data. It can also store any "insights" based on the data it has so far.
Each "insight" has tags associated with it so it can be looked up later, and may optionally have
begin/end dates. If an insight is enough to replace the need for a data set, the Agent can instruct
us to drop the dataset from future context. We then return to Step 3.

2) It can return a final response to the user. It can also store "insights" just like above.
