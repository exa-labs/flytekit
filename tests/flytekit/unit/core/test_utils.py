import json
import logging
import os
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import pytest

import flytekit
from flytekit import FlyteContextManager, task
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.utils import ClassDecorator, _dnsify, timeit, str2bool
from flytekit.loggers import ClickHouseTelemetrySink, get_clickhouse_sink, telemetry_logger
from flytekit.tools.translator import get_serializable_task
from tests.flytekit.unit.test_translator import default_img


@pytest.mark.parametrize(
    "input,expected",
    [
        ("test.abc", "test-abc"),
        ("test", "test"),
        ("", ""),
        (".test", "test"),
        ("Test", "test"),
        ("test.", "test"),
        ("test-", "test"),
        ("test$", "test"),
        ("te$t$", "tet"),
        ("t" * 64, f"da4b348ebe-{'t'*52}"),
    ],
)
def test_dnsify(input, expected):
    assert _dnsify(input) == expected


def test_timeit():
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params._decks = []

    from flytekit.deck import DeckField

    with timeit("Set disable_deck to False"):
        kwargs = {}
        kwargs["disable_deck"] = False
        kwargs["deck_fields"] = (DeckField.TIMELINE.value,)

    ctx = FlyteContextManager.current_context()
    time_info_list = ctx.user_space_params.timeline_deck.time_info
    names = [time_info["Name"] for time_info in time_info_list]
    # check if timeit works for flytekit level code
    assert "Set disable_deck to False" in names

    @task(**kwargs)
    def t1() -> int:
        @timeit("Download data")
        def download_data():
            return "1"

        data = download_data()

        with timeit("Convert string to int"):
            return int(data)

    t1()

    time_info_list = flytekit.current_context().timeline_deck.time_info
    names = [time_info["Name"] for time_info in time_info_list]

    # check if timeit works for user level code
    assert "Download data" in names
    assert "Convert string to int" in names


def test_class_decorator():
    class my_decorator(ClassDecorator):
        def __init__(self, func=None, *, foo="baz"):
            self.foo = foo
            super().__init__(func, foo=foo)

        def execute(self, *args, **kwargs):
            return self.task_function(*args, **kwargs)

        def get_extra_config(self):
            return {"foo": self.foo}

    @task
    @my_decorator(foo="bar")
    def t() -> str:
        return "hello world"

    ss = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert t() == "hello world"
    assert t.get_config(settings=ss) == {}

    ts = get_serializable_task(OrderedDict(), ss, t)
    assert ts.template.config == {"foo": "bar"}

    @task
    @my_decorator
    def t() -> str:
        return "hello world"

    ts = get_serializable_task(OrderedDict(), ss, t)
    assert ts.template.config == {"foo": "baz"}


def test_str_2_bool():
    assert str2bool("true")
    assert not str2bool("false")
    assert str2bool("True")
    assert str2bool("t")
    assert not str2bool("f")
    assert str2bool("1")


def test_timeit_telemetry_success_fallback_to_logger():
    with patch.object(telemetry_logger, "info") as mock_info:
        with timeit("test_step"):
            pass

    mock_info.assert_called_once()
    call_args = mock_info.call_args
    assert call_args[0][0] == "flytekit_step"
    extra = call_args[1]["extra"]
    assert extra["event"] == "flytekit_step"
    assert extra["step"] == "test_step"
    assert extra["status"] == "success"
    assert "wall_time_s" in extra
    assert "process_time_s" in extra
    assert extra["wall_time_s"] >= 0
    assert extra["process_time_s"] >= 0
    assert "error_type" not in extra


def test_timeit_telemetry_error_fallback_to_logger():
    with patch.object(telemetry_logger, "info") as mock_info:
        with pytest.raises(ValueError):
            with timeit("failing_step"):
                raise ValueError("test error")

    mock_info.assert_called_once()
    extra = mock_info.call_args[1]["extra"]
    assert extra["status"] == "error"
    assert extra["error_type"] == "ValueError"
    assert extra["step"] == "failing_step"


def test_timeit_telemetry_extras():
    with patch.object(telemetry_logger, "info") as mock_info:
        with timeit("step_with_extras", input_size_bytes=1024, output_count=3):
            pass

    extra = mock_info.call_args[1]["extra"]
    assert extra["input_size_bytes"] == 1024
    assert extra["output_count"] == 3


def test_timeit_telemetry_context_enrichment():
    @task
    def dummy_task() -> int:
        return 42

    with patch.object(telemetry_logger, "info") as mock_info:
        dummy_task()

    calls = [c for c in mock_info.call_args_list if c[0][0] == "flytekit_step"]
    assert len(calls) > 0


def test_telemetry_disabled():
    from flytekit.loggers import _initialize_telemetry_logger

    original_val = os.environ.get("FLYTE_TELEMETRY_ENABLED")
    try:
        os.environ["FLYTE_TELEMETRY_ENABLED"] = "0"
        _initialize_telemetry_logger()
        assert telemetry_logger.level > logging.CRITICAL
    finally:
        if original_val is not None:
            os.environ["FLYTE_TELEMETRY_ENABLED"] = original_val
        else:
            os.environ.pop("FLYTE_TELEMETRY_ENABLED", None)
        _initialize_telemetry_logger()


def test_telemetry_enabled_by_default():
    from flytekit.loggers import _initialize_telemetry_logger

    original_val = os.environ.pop("FLYTE_TELEMETRY_ENABLED", None)
    try:
        _initialize_telemetry_logger()
        assert telemetry_logger.level == logging.INFO
    finally:
        if original_val is not None:
            os.environ["FLYTE_TELEMETRY_ENABLED"] = original_val
        _initialize_telemetry_logger()


def test_timeit_telemetry_json_format():
    handler = logging.StreamHandler()
    from pythonjsonlogger import jsonlogger
    handler.setFormatter(jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s"))

    original_handlers = telemetry_logger.handlers[:]
    original_level = telemetry_logger.level
    telemetry_logger.handlers.clear()
    telemetry_logger.addHandler(handler)
    telemetry_logger.setLevel(logging.INFO)

    try:
        with patch.object(handler, "emit") as mock_emit:
            with timeit("json_test_step"):
                pass

        mock_emit.assert_called()
        record = mock_emit.call_args[0][0]
        formatted = handler.format(record)
        parsed = json.loads(formatted)
        assert parsed["event"] == "flytekit_step"
        assert parsed["step"] == "json_test_step"
        assert parsed["status"] == "success"
    finally:
        telemetry_logger.handlers.clear()
        for h in original_handlers:
            telemetry_logger.addHandler(h)
        telemetry_logger.setLevel(original_level)


def test_timeit_all_steps_in_task_execution():
    @task
    def add_one(x: int) -> int:
        return x + 1

    with patch.object(telemetry_logger, "info") as mock_info:
        result = add_one(x=5)

    assert result == 6

    step_names = [
        c[1]["extra"]["step"]
        for c in mock_info.call_args_list
        if c[0][0] == "flytekit_step"
    ]
    assert "pre_execute" in step_names
    assert "Execute user level code" in step_names
    assert "post_execute" in step_names


def test_clickhouse_sink_disabled_without_url():
    sink = ClickHouseTelemetrySink()
    assert not sink.enabled


def test_clickhouse_sink_enabled_with_url():
    orig = os.environ.get("FLYTE_TELEMETRY_CLICKHOUSE_URL")
    try:
        os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = "https://ch.example.com:8443"
        sink = ClickHouseTelemetrySink()
        assert sink.enabled
    finally:
        if orig is not None:
            os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = orig
        else:
            os.environ.pop("FLYTE_TELEMETRY_CLICKHOUSE_URL", None)


def test_clickhouse_sink_send_noop_when_disabled():
    sink = ClickHouseTelemetrySink()
    with patch("flytekit.loggers.threading.Thread") as mock_thread:
        sink.send({"event": "test"})
    mock_thread.assert_not_called()


def test_clickhouse_sink_send_fires_background_thread():
    orig = os.environ.get("FLYTE_TELEMETRY_CLICKHOUSE_URL")
    try:
        os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = "https://ch.example.com:8443"
        sink = ClickHouseTelemetrySink()
        with patch("flytekit.loggers.threading.Thread") as mock_thread:
            sink.send({"event": "flytekit_step", "step": "test1"})
        mock_thread.assert_called_once()
        call_kwargs = mock_thread.call_args[1]
        assert call_kwargs["target"] == sink._post_row
        assert call_kwargs["daemon"] is True
    finally:
        if orig is not None:
            os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = orig
        else:
            os.environ.pop("FLYTE_TELEMETRY_CLICKHOUSE_URL", None)


def test_clickhouse_sink_post_row_sends_json():
    orig = os.environ.get("FLYTE_TELEMETRY_CLICKHOUSE_URL")
    try:
        os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = "https://ch.example.com:8443"
        os.environ["FLYTE_TELEMETRY_CLICKHOUSE_USER"] = "testuser"
        os.environ["FLYTE_TELEMETRY_CLICKHOUSE_PASSWORD"] = "testpass"
        sink = ClickHouseTelemetrySink()

        event = {"event": "flytekit_step", "step": "s1", "wall_time_s": 0.1}

        with patch("flytekit.loggers.urllib.request.urlopen") as mock_urlopen:
            sink._post_row(event)

        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        from urllib.parse import unquote
        decoded_url = unquote(req.full_url)
        assert "INSERT INTO" in decoded_url
        assert "FORMAT JSONEachRow" in decoded_url
        parsed = json.loads(req.data.decode("utf-8"))
        assert parsed["step"] == "s1"
    finally:
        if orig is not None:
            os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = orig
        else:
            os.environ.pop("FLYTE_TELEMETRY_CLICKHOUSE_URL", None)
        os.environ.pop("FLYTE_TELEMETRY_CLICKHOUSE_USER", None)
        os.environ.pop("FLYTE_TELEMETRY_CLICKHOUSE_PASSWORD", None)


def test_timeit_routes_to_clickhouse_sink():
    mock_sink = MagicMock()
    mock_sink.enabled = True
    with patch("flytekit.core.utils.get_clickhouse_sink", return_value=mock_sink):
        with timeit("ch_step"):
            pass

    mock_sink.send.assert_called_once()
    event = mock_sink.send.call_args[0][0]
    assert event["step"] == "ch_step"
    assert event["status"] == "success"
    assert "wall_time_s" in event


def test_timeit_routes_to_clickhouse_on_error():
    mock_sink = MagicMock()
    mock_sink.enabled = True
    with patch("flytekit.core.utils.get_clickhouse_sink", return_value=mock_sink):
        with pytest.raises(RuntimeError):
            with timeit("ch_error_step"):
                raise RuntimeError("boom")

    event = mock_sink.send.call_args[0][0]
    assert event["status"] == "error"
    assert event["error_type"] == "RuntimeError"


def test_clickhouse_sink_post_row_silences_errors():
    orig = os.environ.get("FLYTE_TELEMETRY_CLICKHOUSE_URL")
    try:
        os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = "https://ch.example.com:8443"
        sink = ClickHouseTelemetrySink()
        with patch("flytekit.loggers.urllib.request.urlopen", side_effect=Exception("network down")):
            sink._post_row({"event": "test"})
    finally:
        if orig is not None:
            os.environ["FLYTE_TELEMETRY_CLICKHOUSE_URL"] = orig
        else:
            os.environ.pop("FLYTE_TELEMETRY_CLICKHOUSE_URL", None)


def test_default_clickhouse_sink_is_disabled():
    sink = get_clickhouse_sink()
    assert sink is None or not sink.enabled
