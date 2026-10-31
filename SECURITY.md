# Security

Please report security problems privately: email **security@vectrojs.dev**,
or use the repository's **Security** tab > **Report a vulnerability** on
GitHub. Don't open a public issue for them.

What counts: anything that lets a client of `vectro serve` (HTTP, PostgreSQL,
Flight SQL) or `vectro-mcp` read files outside the served folder, write
anything, get past a password or token, or crash the server with a request.

The servers listen on 127.0.0.1 unless `--host` is given, and refuse other
machines without a password or token. They only read.
