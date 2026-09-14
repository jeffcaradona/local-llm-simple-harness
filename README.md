# local-llm-simple-harness

Small Node.js learning project for sending one file and one review request to a local OpenAI-compatible model.

## Requirements

- Node.js 22+
- No external runtime dependencies

## First milestone: one script

The script `review.js` does exactly one request:

1. Reads one file you choose.
2. Combines it with your review request.
3. Calls your configured local `/chat/completions` endpoint.
4. Prints plain-text Markdown output.

## Configure environment

Do not commit secrets. `.env` is already ignored by Git.

PowerShell setup:

```powershell
Copy-Item .env.example .env
```

Then edit `.env` and set at least:

```dotenv
HARNESS_MODEL_BASE_URL=http://127.0.0.1:11434/v1
HARNESS_MODEL_ID=local-model
HARNESS_MODEL_API_KEY=
HARNESS_MODEL_AUTH_SCHEME=bearer
HARNESS_MODEL_TIMEOUT_MS=180000
```

Notes:

- `HARNESS_MODEL_BASE_URL` can include a path prefix like `/v1`; the script appends `chat/completions` correctly.
- `HARNESS_MODEL_API_KEY` is optional for local providers.
- `HARNESS_MODEL_AUTH_SCHEME` supports `bearer` (default) and `basic-password`.
- For llama-swap setups that work with `curl -u ":$K"`, use `HARNESS_MODEL_AUTH_SCHEME=basic-password`.

## Usage

```powershell
node --env-file=.env review.js ./review.js "Review error handling and asynchronous behavior."
```

Help:

```powershell
node review.js --help
```
