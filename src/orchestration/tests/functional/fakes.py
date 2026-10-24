"""Test doubles for the activities' external dependencies."""


class FakeS3:
    """The one boto3 surface collect_outputs uses: a list_objects_v2
    paginator, returning the given keys in pages of `page_size`."""

    def __init__(self, keys: list[str], page_size: int = 2):
        self.keys = keys
        self.page_size = page_size
        self.calls: list[dict] = []

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        matching = [k for k in self.keys if k.startswith(kwargs["Prefix"])]
        for i in range(0, max(len(matching), 1), self.page_size):
            chunk = matching[i : i + self.page_size]
            yield {"Contents": [{"Key": k} for k in chunk]} if chunk else {}
