# pylint: disable=redefined-outer-name

import json
import tempfile
import multiprocessing as mp

import pytest
from pkg_resources import resource_filename

from rma.tasks.base import Worker
from rma.utils import config_logging
from rma.tasks.sources import CSVSource
from rma.tasks.transforms import FilterColumn
from rma.tasks.sinks import FileSink, PrintSink


@pytest.fixture
def comments_source():
    def _run():
        thing = CSVSource(
            resource_filename("rma", "../tests/resources/comments.csv"),
            addrout="tcp://*:5550",
            addrsync="tcp://*:5551",
            nsubs=1,
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def filter_cols():
    def _run():
        thing = Worker(
            subaddr="tcp://localhost:5550",
            reqaddr="tcp://localhost:5551",
            pubaddr="tcp://*:5552",
            subsyncaddr="tcp://*:5553",
            nsubs=1,
            executor_cls=FilterColumn,
            executor_kwargs={"columns": ["body", "permalink"]},
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def file_sink():
    tmp_out = tempfile.NamedTemporaryFile("a", delete=False)

    def _run():
        sink = FileSink(
            tmp_out.name, addrin="tcp://localhost:5552", syncaddr="tcp://localhost:5553"
        )
        sink.run()

    return tmp_out.name, mp.Process(target=_run)


@pytest.fixture
def print_sink():
    def _run():
        sink = PrintSink(addrin="tcp://localhost:5552", syncaddr="tcp://localhost:5553")
        sink.run()

    return mp.Process(target=_run)


def setup_function(function):
    print("setting up", function)
    config_logging(1, False)


def test_dag_1(comments_source, filter_cols, file_sink):
    out_file, file_sink_p = file_sink

    comments_source.start()
    filter_cols.start()
    file_sink_p.start()

    comments_source.join()
    filter_cols.join()
    file_sink_p.join()

    messages = []
    with open(out_file) as f:
        for line in f:
            messages.append(json.loads(line))

    messages = sorted(messages, key=lambda x: x["permalink"])

    assert messages == [
        {
            "body": "Comment7",
            "permalink": "https://old.reddit.com/r/meirl/comments/tswh3j/meirl/i2x23wn/",
        },
        {
            "body": "Comment6",
            "permalink": "https://old.reddit.com/r/meirl/comments/tswh3j/meirl/i2x26cp/",
        },
        {
            "body": "Comment4",
            "permalink": "https://old.reddit.com/r/meirl/comments/tswh3j/meirl/i2x28i1/",
        },
        {
            "body": "Comment1",
            "permalink": "https://old.reddit.com/r/meirl/comments/tswh3j/meirl/i2x2j0g/",
        },
        {
            "body": "Comment5",
            "permalink": "https://old.reddit.com/r/meirl/comments/tt5aas/meirl/i2x27un/",
        },
        {
            "body": "Comment3",
            "permalink": "https://old.reddit.com/r/meirl/comments/tt5aas/meirl/i2x2dv0/",
        },
        {
            "body": "Comment2",
            "permalink": "https://old.reddit.com/r/meirl/comments/tt9v20/meirl/i2x2hqk/",
        },
        {
            "body": "Comment3",
            "permalink": "https://old.reddit.com/r/meirl/comments/ttbnpo/meirl/i2x2d15/",
        },
    ]
