#!/usr/bin/env python3
"""Atomically maintain Warden's root-owned AMP guild registry."""

from __future__ import annotations

import argparse
import fcntl
import grp
import json
import os
import stat
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import dotenv_values  # noqa: E402
from utils.amp_registry import (  # noqa: E402
    REGISTRY_PATH,
    parse_legacy_amp_guild_bindings,
    read_secure_amp_registry_policy,
    registry_document,
)


LOCK_PATH = REGISTRY_PATH.with_name('.amp-guild-bindings.lock')


def _ensure_secure_parent():
    REGISTRY_PATH.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    metadata = REGISTRY_PATH.parent.stat()
    if metadata.st_uid != 0 or metadata.st_mode & 0o022:
        raise SystemExit('Registry directory must be root-owned and not group/world writable.')


def _write_registry(bindings, public_passwords):
    content = registry_document(
        {key: list(value) for key, value in bindings.items()},
        {key: list(value) for key, value in public_passwords.items()},
    )
    _ensure_secure_parent()
    group_id = grp.getgrnam('www-data').gr_gid
    fd, temporary = tempfile.mkstemp(prefix='.amp-guild-bindings-', dir=REGISTRY_PATH.parent)
    try:
        os.fchmod(fd, 0o640)
        os.fchown(fd, 0, group_id)
        with os.fdopen(fd, 'w', encoding='utf-8', closefd=False) as output:
            output.write(content)
            output.flush()
            os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary, REGISTRY_PATH)
        directory_fd = os.open(REGISTRY_PATH.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _load_existing():
    policy = read_secure_amp_registry_policy(REGISTRY_PATH)
    if policy is None:
        return {}, {}
    return policy.bindings, policy.public_passwords


def main():
    if os.geteuid() != 0:
        raise SystemExit('This command must run as root.')

    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest='action', required=True)
    bind = subparsers.add_parser('bind')
    bind.add_argument('instance')
    bind.add_argument('guild_id', type=int)
    unbind = subparsers.add_parser('unbind')
    unbind.add_argument('instance')
    unbind.add_argument('guild_id', type=int, nargs='?')
    allow_password = subparsers.add_parser('allow-public-password')
    allow_password.add_argument('instance')
    allow_password.add_argument('guild_id', type=int)
    deny_password = subparsers.add_parser('deny-public-password')
    deny_password.add_argument('instance')
    deny_password.add_argument('guild_id', type=int)
    bootstrap = subparsers.add_parser('bootstrap-env')
    bootstrap.add_argument('--env-file', type=Path, default=Path('/etc/casual-heroes/warden.env'))
    subparsers.add_parser('list')
    args = parser.parse_args()

    _ensure_secure_parent()
    lock_fd = os.open(
        LOCK_PATH,
        os.O_RDWR | os.O_CREAT | getattr(os, 'O_CLOEXEC', 0) | getattr(os, 'O_NOFOLLOW', 0),
        0o600,
    )
    lock_metadata = os.fstat(lock_fd)
    if (
        not stat.S_ISREG(lock_metadata.st_mode)
        or lock_metadata.st_uid != 0
        or lock_metadata.st_mode & 0o022
    ):
        os.close(lock_fd)
        raise SystemExit(
            'Registry lock must be a root-owned regular file and not group/world writable.'
        )
    with os.fdopen(lock_fd, 'a+', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        bindings, public_passwords = _load_existing()
        if args.action == 'bind':
            guild_ids = set(bindings.get(args.instance, ()))
            guild_ids.add(args.guild_id)
            bindings[args.instance] = frozenset(guild_ids)
            _write_registry(bindings, public_passwords)
        elif args.action == 'unbind':
            if args.guild_id is None:
                bindings.pop(args.instance, None)
                public_passwords.pop(args.instance, None)
            else:
                guild_ids = set(bindings.get(args.instance, ()))
                guild_ids.discard(args.guild_id)
                if guild_ids:
                    bindings[args.instance] = frozenset(guild_ids)
                else:
                    bindings.pop(args.instance, None)
                password_guild_ids = set(public_passwords.get(args.instance, ()))
                password_guild_ids.discard(args.guild_id)
                if password_guild_ids:
                    public_passwords[args.instance] = frozenset(password_guild_ids)
                else:
                    public_passwords.pop(args.instance, None)
            _write_registry(bindings, public_passwords)
        elif args.action == 'allow-public-password':
            if args.guild_id not in bindings.get(args.instance, ()):
                raise SystemExit('Instance must be bound to the guild first.')
            guild_ids = set(public_passwords.get(args.instance, ()))
            guild_ids.add(args.guild_id)
            public_passwords[args.instance] = frozenset(guild_ids)
            _write_registry(bindings, public_passwords)
        elif args.action == 'deny-public-password':
            guild_ids = set(public_passwords.get(args.instance, ()))
            guild_ids.discard(args.guild_id)
            if guild_ids:
                public_passwords[args.instance] = frozenset(guild_ids)
            else:
                public_passwords.pop(args.instance, None)
            _write_registry(bindings, public_passwords)
        elif args.action == 'bootstrap-env':
            if bindings:
                raise SystemExit('Registry already exists and is non-empty; refusing bootstrap.')
            legacy = dotenv_values(args.env_file).get('WARDEN_AMP_GUILD_BINDINGS')
            bindings = parse_legacy_amp_guild_bindings(legacy)
            if not bindings:
                raise SystemExit('No WARDEN_AMP_GUILD_BINDINGS mapping found.')
            _write_registry(bindings, {})
        else:
            print(json.dumps({
                'bindings': {key: sorted(value) for key, value in sorted(bindings.items())},
                'public_passwords': {
                    key: sorted(value) for key, value in sorted(public_passwords.items())
                },
            }, indent=2))


if __name__ == '__main__':
    main()
