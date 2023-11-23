#!/usr/bin/env python3

import random
import time

import db


def test(*args, **kwargs):
    time.sleep(random.random() * 10)
    if random.random() < 0.3:
        print("FAIL", args, kwargs)
        duh  # noqa
    print("Success", args, kwargs)
    return "Passed"


def main():
    tries = 3
    timeout = 5
    db.fetchall(
        "insert into job(queue, payload, tries, timeout) values (%s, %s, %s, %s)",
        ("default", {"func": "test.test", "args": [], "kwargs": {}}, tries, timeout),
    )
    db.commit()


if __name__ == "__main__":
    main()
