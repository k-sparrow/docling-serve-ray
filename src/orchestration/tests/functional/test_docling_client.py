"""Request bodies checked against the models docling-serve validates with.

Those models ignore unknown keys instead of rejecting them: a misnamed field
("convert_options" for "options") or a misspelled option is silently dropped
and the server falls back to its defaults. Validating alone therefore can't
catch it -- a body must also survive a validate/dump round trip unchanged,
which fails on any key the server would drop.
"""

import pytest
from docling.datamodel.service.requests import BatchConvertSourcesRequest

from orchestration.docling_client import build_batch_convert_body, callback_specs

WORKFLOW_ID = "convert:docling-input/incoming/a b.pdf"


def _as_server_sees_it(body: dict) -> dict:
    request = BatchConvertSourcesRequest.model_validate(body)
    return request.model_dump(mode="json", exclude_unset=True)


def _body(settings, options: dict, callbacks: list[dict] | None = None) -> dict:
    return build_batch_convert_body(
        settings,
        bucket="docling-input",
        key="incoming/a b.pdf",
        options=options,
        callbacks=callback_specs(settings, WORKFLOW_ID)
        if callbacks is None
        else callbacks,
    )


@pytest.mark.parametrize(
    "options",
    [
        {"to_formats": ["json", "md"]},
        {"to_formats": ["json", "md", "html"], "do_ocr": False, "page_range": [1, 5]},
    ],
)
def test_batch_body_matches_server_model(settings, options):
    body = _body(settings, options)

    assert _as_server_sees_it(body) == body


def test_escaped_workflow_id_survives_the_servers_url_parsing(settings):
    # docling stores the callback URL as a pydantic AnyUrl; any
    # normalization of the escaped workflow id would send the relay a
    # different id than the workflow's own.
    body = _body(settings, {"to_formats": ["json"]})

    url = _as_server_sees_it(body)["callbacks"][0]["url"]

    assert url == (
        "http://relay.test/v1/callbacks/"
        "convert%3Adocling-input%2Fincoming%2Fa%20b.pdf"
    )


def test_body_without_callbacks_matches_server_model(settings):
    body = _body(settings, {"to_formats": ["json"]}, callbacks=[])

    assert _as_server_sees_it(body) == body


def test_round_trip_catches_keys_the_server_would_drop(settings):
    body = _body(settings, {"do_ocrr": True})
    misnamed = {**body, "convert_options": body["options"]}
    del misnamed["options"]

    assert _as_server_sees_it(body) != body
    assert _as_server_sees_it(misnamed) != misnamed
