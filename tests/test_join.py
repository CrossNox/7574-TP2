# pylint: disable=redefined-outer-name

import json
import tempfile
import multiprocessing as mp

import pytest
from pkg_resources import resource_filename

from rma.tasks.base import Joiner
from rma.tasks.sinks import FileSink
from rma.utils import config_logging
from rma.tasks.joiners import KeyJoin
from rma.tasks.sources import CSVSource


@pytest.fixture
def source1():
    def _run():
        thing = CSVSource(
            resource_filename("rma", "../tests/resources/test1.csv"),
            addrout="tcp://*:6050",
            addrsync="tcp://*:6051",
            nsubs=1,
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def source2():
    def _run():
        thing = CSVSource(
            resource_filename("rma", "../tests/resources/test2.csv"),
            addrout="tcp://*:6052",
            addrsync="tcp://*:6053",
            nsubs=1,
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def join():
    def _run():
        thing = Joiner(
            pubaddr="tcp://*:6054",
            subsyncaddr="tcp://*:6055",
            nsubs=1,
            inputs=[
                ("tcp://localhost:6051", "tcp://localhost:6050"),
                ("tcp://localhost:6053", "tcp://localhost:6052"),
            ],
            executor_cls=KeyJoin,
            executor_kwargs={"ninputs": 2, "key": "key"},
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def file_sink():
    tmp_out = tempfile.NamedTemporaryFile("a", delete=False)

    def _run():
        sink = FileSink(
            tmp_out.name, addrin="tcp://localhost:6054", syncaddr="tcp://localhost:6055"
        )
        sink.run()

    return tmp_out.name, mp.Process(target=_run)


def setup_function(_function):
    config_logging(1, False)


def test_join(source1, source2, join, file_sink):
    out_file, file_sink_p = file_sink

    source1.start()
    source2.start()
    join.start()
    file_sink_p.start()

    source1.join()
    source2.join()
    join.join()
    file_sink_p.join()

    messages = []
    with open(out_file) as f:
        for line in f:
            messages.append(json.loads(line))

    messages = sorted(messages, key=lambda x: int(x["key"]))

    assert messages == [
        {"key": f"{i}", "val1": f"{i}", "val2": f"{i}"} for i in range(1, 16)
    ]
