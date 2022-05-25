# pylint: disable=redefined-outer-name

import json
import tempfile
import multiprocessing as mp

import pytest
from pkg_resources import resource_filename

from rma.tasks.base import Worker
from rma.tasks.sinks import FileSink
from rma.utils import config_logging
from rma.tasks.sources import CSVSource
from rma.tasks.filters import FilterPostsScoreAboveMean
from rma.tasks.transforms import FilterColumn, PostsScoreMean

# source >> filter_col1
# source >> filter_col2
# posts_mean << filter_col2
# filter_posts << [filter_col1, posts_mean]
# sink1 << filter_posts
# sink2 <<  posts_mean


@pytest.fixture
def posts_source():
    def _run():
        thing = CSVSource(
            resource_filename("rma", "../tests/resources/posts.csv"),
            addrout="tcp://*:6050",
            addrsync="tcp://*:6051",
            nsubs=2,
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def filter_posts_cols1():
    def _run():
        thing = Worker(
            subaddr="tcp://localhost:6050",
            reqaddr="tcp://localhost:6051",
            pubaddr="tcp://*:6052",
            subsyncaddr="tcp://*:6053",
            nsubs=1,
            executor_cls=FilterColumn,
            executor_kwargs={"columns": ["id", "score", "url"]},
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def filter_posts_cols2():
    def _run():
        thing = Worker(
            subaddr="tcp://localhost:6050",
            reqaddr="tcp://localhost:6051",
            pubaddr="tcp://*:6054",
            subsyncaddr="tcp://*:6055",
            nsubs=1,
            executor_cls=FilterColumn,
            executor_kwargs={"columns": ["id", "score"]},
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def score_mean():
    def _run():
        thing = Worker(
            subaddr="tcp://localhost:6054",
            reqaddr="tcp://localhost:6055",
            pubaddr="tcp://*:6056",
            subsyncaddr="tcp://*:6057",
            nsubs=2,
            executor_cls=PostsScoreMean,
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def filter_posts_above_mean_score():
    def _run():
        thing = Worker(
            subaddr="tcp://localhost:6052",
            reqaddr="tcp://localhost:6053",
            pubaddr="tcp://*:6058",
            subsyncaddr="tcp://*:6059",
            nsubs=1,
            executor_cls=FilterPostsScoreAboveMean,
            deps=[("mean", "tcp://localhost:6057", "tcp://localhost:6056")],
        )
        thing.run()

    return mp.Process(target=_run)


@pytest.fixture
def file_sink_1():
    tmp_out = tempfile.NamedTemporaryFile("a", delete=False)

    def _run():
        sink = FileSink(
            tmp_out.name, addrin="tcp://localhost:6058", syncaddr="tcp://localhost:6059"
        )
        sink.run()

    return tmp_out.name, mp.Process(target=_run)


@pytest.fixture
def file_sink_2():
    tmp_out = tempfile.NamedTemporaryFile("a", delete=False)

    def _run():
        sink = FileSink(
            tmp_out.name, addrin="tcp://localhost:6056", syncaddr="tcp://localhost:6057"
        )
        sink.run()

    return tmp_out.name, mp.Process(target=_run)


def setup_function(_function):
    config_logging(2, False)


def test_dag_deps(
    posts_source,
    filter_posts_cols1,
    filter_posts_cols2,
    score_mean,
    filter_posts_above_mean_score,
    file_sink_1,
    file_sink_2,
):
    out_file_1, file_sink_1_p = file_sink_1
    _out_file_2, file_sink_2_p = file_sink_2

    posts_source.start()
    filter_posts_cols1.start()
    filter_posts_cols2.start()
    score_mean.start()
    filter_posts_above_mean_score.start()
    file_sink_1_p.start()
    file_sink_2_p.start()

    # print(f"reading {out_file_1}")
    # with open(out_file_1) as f:
    #    for line in f:
    #        print(line)

    posts_source.join()
    filter_posts_cols1.join()
    filter_posts_cols2.join()
    score_mean.join()
    filter_posts_above_mean_score.join()
    file_sink_1_p.join()
    file_sink_2_p.join()

    messages = []
    with open(out_file_1) as f:
        for line in f:
            messages.append(json.loads(line))

    messages = sorted(messages, key=lambda x: x["id"])

    assert messages == [
        {"score": "309", "id": "ttbnpo", "url": "https://i.redd.it/gnf42s5lqsq81.jpg"},
        {"score": "144", "id": "ttbv93", "url": "https://i.redd.it/bxfvfw1issq81.jpg"},
        {"score": "605", "id": "ttd04u", "url": "https://i.redd.it/p86ehgxw2tq81.jpg"},
    ]
