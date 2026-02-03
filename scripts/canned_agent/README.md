# Canned Agent Responses

This directory contains canned scenario folders you can pass to `scripts/ask_agent.py`
via the `--stub-file` (now a path to an `index.json`) flag for offline testing. Each scenario folder
contains an `index.json` that maps keyword keys to relative response files within the same folder.

- `earnings/index.json`: indexes the earnings-focused responses under `earnings/responses/`.
- `macro/index.json`: indexes the FX/market responses under `macro/responses/`.
- `crypto/index.json`: indexes crypto scenarios under `crypto/responses/`.
