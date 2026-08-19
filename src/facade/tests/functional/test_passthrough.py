"""Proves the facade forwards every conversion option it doesn't itself need
to intercept untouched, not just `to_formats` -- which used to be the only
one the facade declared explicitly (see project memory: main.py originally
had exactly two `Form()` parameters, `to_formats` and `target_type`, silently
dropping every one of docling-serve's other ~45 ConvertDocumentsOptions
fields -- do_ocr, page_range, ocr_lang, pipeline, and so on -- for any client
that sent them). Two layers: `TestBuildConvertOptionsPassthrough` proves the
pure `build_convert_options` mapping function in isolation; `TestHttpLayerPassthrough`
proves the same thing through the real HTTP/Form-parsing route, end to end.
"""

from starlette.datastructures import FormData

from facade.tests.functional.fakes import fake_response
from facade.utils import build_convert_options


def _form(pairs: list[tuple[str, str]]) -> FormData:
    return FormData(pairs)


class TestBuildConvertOptionsPassthrough:
    def test_a_scalar_field_passes_through_as_a_bare_value_not_a_list(self):
        options = build_convert_options(_form([("do_ocr", "false")]))
        assert options == {"do_ocr": "false"}

    def test_a_field_sent_twice_becomes_a_list(self):
        # Nothing in the facade's small hardcoded set knows about a made-up
        # field name -- this proves list-on-repetition is generic behavior,
        # not special-cased per field.
        options = build_convert_options(
            _form([("custom_thing", "a"), ("custom_thing", "b")])
        )
        assert options == {"custom_thing": ["a", "b"]}

    def test_a_known_array_field_stays_a_list_even_with_a_single_value(self):
        # page_range is 2-item-array-typed in docling-serve's own schema --
        # a client sending it exactly once must not be collapsed to a bare
        # scalar, which would fail validation on docling-serve's side.
        options = build_convert_options(_form([("ocr_lang", "eng")]))
        assert options == {"ocr_lang": ["eng"]}

    def test_an_arbitrary_field_the_facade_has_never_heard_of_still_passes_through(
        self,
    ):
        # The whole point: build_convert_options doesn't need to know what a
        # field means to forward it. Only the array-vs-scalar distinction
        # for single-occurrence fields needs schema knowledge at all.
        options = build_convert_options(
            _form([("some_future_docling_option", "value")])
        )
        assert options == {"some_future_docling_option": "value"}

    def test_files_and_target_type_are_excluded_not_forwarded(self):
        options = build_convert_options(
            _form([("files", "irrelevant"), ("target_type", "zip"), ("do_ocr", "true")])
        )
        assert options == {"do_ocr": "true"}

    def test_empty_form_produces_empty_options(self):
        assert build_convert_options(_form([])) == {}

    def test_a_realistic_multi_field_submission_all_survives_together(self):
        options = build_convert_options(
            _form(
                [
                    ("files", "irrelevant"),
                    ("target_type", "inbody"),
                    ("to_formats", "md"),
                    ("to_formats", "json"),
                    ("do_ocr", "false"),
                    ("force_ocr", "true"),
                    ("ocr_engine", "easyocr"),
                    ("page_range", "1"),
                    ("page_range", "3"),
                    ("pipeline", "vlm"),
                ]
            )
        )
        assert options == {
            "to_formats": ["md", "json"],
            "do_ocr": "false",
            "force_ocr": "true",
            "ocr_engine": "easyocr",
            "page_range": ["1", "3"],
            "pipeline": "vlm",
        }


class TestHttpLayerPassthrough:
    def test_a_representative_sample_of_options_reaches_docling_serve_untouched(
        self, client, mock_docling_client
    ):
        mock_docling_client.post.return_value = fake_response(
            200,
            json={
                "task_id": "opts-task",
                "task_type": "convert",
                "task_status": "pending",
            },
        )

        response = client.post(
            "/v1/convert/file/async",
            files=[("files", ("a.pdf", b"%PDF-1.4 fake", "application/pdf"))],
            data={
                "to_formats": ["md"],
                "do_ocr": "false",
                "ocr_engine": "tesseract",
                "page_range": ["1", "2"],
                "ocr_lang": "eng",
            },
        )

        assert response.status_code == 200
        posted = mock_docling_client.post.call_args.kwargs["json"]["options"]
        assert posted == {
            "to_formats": ["md"],
            "do_ocr": "false",
            "ocr_engine": "tesseract",
            "page_range": ["1", "2"],
            "ocr_lang": ["eng"],
        }

    def test_options_are_not_required_defaults_still_work(
        self, client, mock_docling_client
    ):
        # A client sending nothing but a file must still work -- absence of
        # every option is itself a case build_convert_options has to handle
        # (empty dict, not a KeyError or a forced default value the facade
        # invents on docling-serve's behalf).
        mock_docling_client.post.return_value = fake_response(
            200,
            json={
                "task_id": "bare-task",
                "task_type": "convert",
                "task_status": "pending",
            },
        )

        response = client.post(
            "/v1/convert/file/async",
            files=[("files", ("a.pdf", b"%PDF-1.4 fake", "application/pdf"))],
        )

        assert response.status_code == 200
        posted = mock_docling_client.post.call_args.kwargs["json"]["options"]
        assert posted == {}
