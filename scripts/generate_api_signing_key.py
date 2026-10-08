#!/usr/bin/env python3
"""Generate an Ed25519 key pair for QuestLog-to-Warden API signing."""

import argparse
import base64
import json
import re

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


SCOPES = [
    "guilds.read",
    "guilds.sync",
    "moderation.write",
    "creator.write",
    "creator.network.write",
]


def b64url(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def main():
    parser = argparse.ArgumentParser(
        description="Generate raw base64url Ed25519 keys for the Warden API"
    )
    parser.add_argument(
        "--key-id",
        default="questlog-web-v1",
        help="non-secret identifier recorded in Warden audit logs",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", args.key_id):
        parser.error("--key-id must use 1-64 letters, digits, dots, underscores, or hyphens")

    private_key = Ed25519PrivateKey.generate()
    private_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    trusted_signers = {
        args.key_id: {
            "public_key": b64url(public_bytes),
            "scopes": SCOPES,
        }
    }

    print("Store only these two values on the QuestLog web host:")
    print(f"WARDEN_API_SIGNING_KEY_ID={args.key_id}")
    print(f"WARDEN_API_SIGNING_PRIVATE_KEY={b64url(private_bytes)}")
    print("\nStore only this public configuration on the Warden host:")
    print("WARDEN_API_TRUSTED_SIGNERS=" + json.dumps(trusted_signers, separators=(",", ":")))


if __name__ == "__main__":
    main()
