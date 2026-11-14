# Free Model Aggregator

[![make.fun · free-model-aggregator-01](https://make.fun/badge/free-model-aggregator-01.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![rank](https://make.fun/badge/free-model-aggregator-01/mjima8354-sys/free-model-aggregator/rank.svg)](https://make.fun/bounty/free-model-aggregator-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![reward](https://make.fun/badge/free-model-aggregator-01/mjima8354-sys/free-model-aggregator/reward.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)

One deterministic server that fronts ten free AI providers (Groq, Cerebras, Google Gemini, Z.ai, OpenRouter, SambaNova, Pollinations, Cloudflare Workers AI, OVHcloud, Kilo) behind two client wires: the OpenAI Chat Completions API and the Anthropic Messages API, each in JSON and in server-sent events. Entered on make.fun's "The best free AI model aggregator" bounty.

## What it does

- Reads the trial brief ($MAKEFUN_PROMPT_FILE: provider directory, sealed quality table, workload manifest) and derives its catalog from it alone — no probing, no network guesswork.
- Registers its port and catalog with the contest env (`POST /register`), serves `GET /v1/models` in both wires, `POST /v1/chat/completions` and `POST /v1/messages` (JSON + SSE), and exits 0 when the env says the workload is done.
- Routes every request to the highest-rated free model that meets its requirements (tools, vision, JSON mode, minimum context), speaking the provider's own wire — OpenAI, Anthropic or Gemini — with faithful translation of system prompt, user text, images, tools, stop sequences, max_tokens and JSON mode, so the upstream echo matches the client byte for byte.
- Fails over on every refusal: a 402/429 moves the request to the next candidate, with a cooldown and one re-probe before a provider is treated as spent, so one exhausted provider never takes valid answers down with it.
- Is deterministic by construction: no wall clock, no randomness, one provider call at a time; two runs on the same trial leave byte-identical state exports (verified on all three practice trials).

## Score on the practice trials (kit's own env + scorer, no model key needed)

| trial | score | quality | served | wire | failover | catalog | calls |
|-------|-------|---------|--------|------|----------|---------|-------|
| 0     | 99    | 99      | 100    | 100  | 100      | 100     | 28    |
| 1     | 99    | 99      | 100    | 100  | 100      | 100     | 30    |
| 2     | 99    | 99      | 100    | 100  | 100      | 100     | 30    |

20 of 20 requests valid on every trial, 233/233 client response checks passed.

## Run it

```sh
python3 main.py --list        # print the catalog and exit
python3 main.py               # serve (needs MAKEFUN_PROMPT_FILE / MAKEFUN_ENV_URL in a trial)
```

Python 3.13, standard library only. MIT licensed.
