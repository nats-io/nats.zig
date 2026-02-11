# NKeys and JWTs

NATS supports decentralized authentication using a three-level trust hierarchy:

- **Operator** - Top-level entity that manages accounts
- **Account** - Groups users and defines resource limits
- **User** - Authenticates to NATS with permissions

Each entity has an NKey keypair (Ed25519). Operators sign account JWTs,
and accounts sign user JWTs. This creates a chain of trust without
requiring a central authority.

## How It Works

1. Generate an **operator** keypair (prefix `SO`)
2. Generate an **account** keypair (prefix `SA`)
3. The operator signs an **account JWT** containing the account's public key
4. Generate a **user** keypair (prefix `SU`)
5. The account signs a **user JWT** with publish/subscribe permissions
6. Format a **credentials file** (`.creds`) containing the user JWT and seed

The credentials file is what a NATS client uses to authenticate. The server
validates the JWT signature chain back to a trusted operator.

## Running

No NATS server required - this is a pure cryptography example.

```sh
zig build run-nbe-auth-nkeys-jwts
```

## Output

```
== Operator ==
operator public key: <dynamic>
operator seed:       <dynamic>

== Account ==
account public key: <dynamic>
account seed:       <dynamic>

account JWT:
<dynamic>

== User ==
user public key: <dynamic>
user seed:       <dynamic>

user JWT:
<dynamic>

== Credentials File ==
-----BEGIN NATS USER JWT-----
<dynamic>
------END NATS USER JWT------

************************* IMPORTANT *************************
  NKEY Seed printed below can be used to sign and prove identity.
  NKEYs are sensitive and should be treated as secrets.

  ************************************************************

-----BEGIN USER NKEY SEED-----
<dynamic>
------END USER NKEY SEED------
```

## What's Happening

1. Three NKey keypairs are generated: operator, account, and user. Each uses
   Ed25519 with type-specific base32 prefixes (`SO`, `SA`, `SU`).
2. An account JWT is created with default limits (unlimited) and signed by the
   operator's private key. The JWT contains the account's public key as subject
   and the operator's public key as issuer.
3. A user JWT is created with publish permissions on `app.>` and subscribe
   permissions on `app.>` and `_INBOX.>`, signed by the account's private key.
4. A credentials file is formatted containing the user JWT and seed. This file
   can be passed to a NATS client via `--creds` for authentication.

## Source

See [nkeys-jwts.zig](nkeys-jwts.zig) for the full example.

Based on [natsbyexample.com/examples/auth/nkeys-jwts](https://natsbyexample.com/examples/auth/nkeys-jwts/go).
