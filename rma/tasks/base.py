import json
from typing import List, Tuple

import zmq

from rma.utils import get_logger

logger = get_logger(__name__)


class VentilatorSource:
    def __init__(
        self,
        subaddr,
        subsyncaddr,
        pushaddr,
        syncaddr,
        sinkaddr,
        nworkers: int,
        subfilter: str = "",
    ):
        self.context = zmq.Context.instance()  # type: ignore

        # SUB where to get data from
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect(subaddr)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, subfilter)

        # SUB sync
        self.subsync = self.context.socket(zmq.REQ)
        self.subsync.connect(subsyncaddr)

        # PUSH where to publish data
        self.push = self.context.socket(zmq.PUSH)
        self.push.bind(pushaddr)

        # REP to sync workers
        self.sync_rep = self.context.socket(zmq.REP)
        self.sync_rep.bind(syncaddr)

        # Number of workers to coordinate
        self.nworkers = nworkers

        # REQ to sync with sink
        self.sink_req = self.context.socket(zmq.REQ)
        self.sink_req.connect(sinkaddr)

        logger.debug("Ventilate source subscribed to %s", subaddr)
        logger.debug("Ventilate syncing to %s", subsyncaddr)
        logger.debug("Ventilate source pushing to %s", pushaddr)
        logger.debug("Ventilate sync addr %s", syncaddr)
        logger.debug("Ventilate looking for sink %s", sinkaddr)

    def run(self):
        logger.debug("Syncing with sink")
        self.sink_req.send(b"")
        self.sink_req.recv()

        logger.debug("Syncing with all workers")
        subs = 0
        while subs < self.nworkers:
            _ = self.sync_rep.recv()
            self.sync_rep.send(b"")
            subs += 1
            logger.debug(f"+1 subscriber ({subs}/{self.nworkers})")

        logger.debug("Syncing with pub")
        self.subsync.send(b"")
        self.subsync.recv()

        while True:
            s = self.sub.recv()

            if s == b"":
                break

            self.push.send(s)

        logger.debug("Sending %s poison pills", self.nworkers)
        for _ in range(self.nworkers):
            self.push.send(b"")


class VentilatorWorker:
    def __init__(
        self, pulladdr, reqaddr, pushaddr, executor_cls, executor_kwargs=None,
    ):
        self.context = zmq.Context.instance()

        # PULL address to get message from
        self.task_pull = self.context.socket(zmq.PULL)
        self.task_pull.connect(pulladdr)

        # Source sync
        self.source_req = self.context.socket(zmq.REQ)
        self.source_req.connect(reqaddr)

        # PUSH addr
        self.push = self.context.socket(zmq.PUSH)
        self.push.connect(pushaddr)

        # Thingy to execute
        if executor_kwargs is None:
            executor_kwargs = dict()
        self.executor = executor_cls(
            task_in=self.task_pull, task_out=self.push, **executor_kwargs
        )

        logger.debug("Worker pulling from %s", pulladdr)
        logger.debug("Worker synced to %s", reqaddr)
        logger.debug("Worker pushing to %s", pushaddr)

    def run(self):
        self.source_req.send(b"")
        self.source_req.recv()
        self.executor.run()
        self.push.send(b"")


class VentilatorSink:
    def __init__(
        self, pulladdr, repaddr, pubaddr, nworkers: int, nsubs: int, subsyncaddr
    ):
        self.context = zmq.Context.instance()  # type: ignore

        # PULL where to get workers results
        self.workers_results = self.context.socket(zmq.PULL)
        self.workers_results.bind(pulladdr)

        # REP to sync with source
        self.source_rep = self.context.socket(zmq.REP)
        self.source_rep.bind(repaddr)

        # PUB where to publish results
        self.pub = self.context.socket(zmq.PUB)
        self.pub.bind(pubaddr)

        # REP to sync subs
        self.syncsubs = self.context.socket(zmq.REP)
        self.syncsubs.bind(subsyncaddr)

        self.nsubs = nsubs

        # Number of workers to keep track of exits
        self.nworkers = nworkers

        logger.debug("Ventilate sink :: pulling from %s", pulladdr)
        logger.debug("Ventilate sink :: source sync at %s", repaddr)
        logger.debug("Ventilate sink :: publishing at %s", pubaddr)
        logger.debug("Ventilate sink :: syncing %s at address %s", nsubs, subsyncaddr)

    def run(self):
        logger.debug("Syncing with source")
        self.source_rep.recv()
        self.source_rep.send(b"")

        logger.debug("Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.debug(f"Ventilate sink :: +1 subscriber ({subs}/{self.nworkers})")

        alive_workers = self.nworkers
        while alive_workers > 0:
            s = self.workers_results.recv()

            if s == b"":
                alive_workers -= 1
            else:
                self.pub.send(s)

        self.pub.send(b"")


class Worker:
    def __init__(
        self,
        subaddr,
        reqaddr,
        pubaddr,
        subsyncaddr,
        nsubs: int,
        executor_cls,
        subfilter="",
        executor_kwargs=None,
        deps: List[Tuple[str, str, str]] = None,
    ):
        self.context = zmq.Context.instance()  # type: ignore

        # SUB where to get data from
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect(subaddr)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, subfilter)

        # REQ to sync with producer
        self.req = self.context.socket(zmq.REQ)
        self.req.connect(reqaddr)

        # PUB to publish transformed data to
        self.pub = self.context.socket(zmq.PUB)
        self.pub.bind(pubaddr)

        # REP to sync with subscribers
        self.syncsubs = self.context.socket(zmq.REP)
        self.syncsubs.bind(subsyncaddr)

        self.nsubs = nsubs

        if deps is None:
            deps = []

        self.deps = self._resolve_deps(deps)

        # Thingy to execute
        if executor_kwargs is None:
            executor_kwargs = dict()
        self.executor = executor_cls(
            **self.deps, task_in=self.sub, task_out=self.pub, **executor_kwargs
        )

        logger.debug("Worker :: subbed to %s with filter '%s'", subaddr, subfilter)
        logger.debug("Worker :: sync with producer at %s", reqaddr)
        logger.debug("Worker :: publishing at %s", pubaddr)
        logger.debug("Worker :: sync with %s clients at address %s", nsubs, subsyncaddr)

    def _resolve_deps(self, deps):
        logger.debug("Worker :: resolving deps")

        final_deps = {}
        _subs = {}

        for dep_name, dep_sync, dep_sub in deps:
            logger.debug("%s result: %s sub: %s", dep_name, dep_sync, dep_sub)

            _sub = zmq.Context.instance().socket(zmq.SUB)
            _sub.connect(dep_sub)
            _sub.setsockopt_string(zmq.SUBSCRIBE, "")
            _subs[dep_name] = _sub

            _dep_sync = zmq.Context.instance().socket(zmq.REQ)
            _dep_sync.connect(dep_sync)
            _dep_sync.send(b"")
            _dep_sync.recv()

        logger.debug("Worker :: Waiting for dependencies results")

        for dep_name, sub in _subs.items():
            final_deps[dep_name] = json.loads(sub.recv().decode())

        return final_deps

    def run(self):
        logger.debug("Worker :: Syncing with producer")
        self.req.send(b"")
        self.req.recv()

        logger.debug("Worker :: Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.debug(f"Worker :: +1 subscriber ({subs}/{self.nsubs})")

        logger.debug("Worker :: Running executor")
        self.executor.run()

        logger.debug("Worker :: Sending poison pill")
        self.pub.send(b"")

        logger.debug("Worker :: Exiting")


class Joiner:
    # Yes, this is so close to a worker
    # Just changes the merging of subs
    # TODO: merge these two things
    def __init__(
        self,
        pubaddr,
        repaddr,
        nsubs: int,
        inputs: List[Tuple[str, str]],
        executor_cls,
        executor_kwargs=None,
    ):
        self.context = zmq.Context.instance()  # type: ignore

        # PUB to publish joined data to
        self.pub = self.context.socket(zmq.PUB)
        self.pub.bind(pubaddr)

        # REP to sync with subscribers
        self.syncsubs = self.context.socket(zmq.REP)
        self.syncsubs.bind(repaddr)
        self.nsubs = nsubs

        # Subscriptions
        self.sub = self.context.socket(zmq.SUB)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self.inputs = inputs
        logger.debug("Joiner :: Syncing with all inputs")
        for dep_sync, dep_sub in self.inputs:
            self.sub.connect(dep_sub)

            _dep_sync = self.context.socket(zmq.REQ)
            _dep_sync.connect(dep_sync)
            _dep_sync.send(b"")
            _dep_sync.recv()

        if executor_kwargs is None:
            executor_kwargs = dict()
        self.executor = executor_cls(
            task_in=self.sub, task_out=self.pub, **executor_kwargs
        )

    def run(self):
        logger.debug("Joiner :: Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.debug(f"Joiner :: +1 subscriber ({subs}/{self.nsubs})")

        logger.debug("Joiner :: Running executor")
        self.executor.run()

        logger.debug("Joiner :: Sending poison pill")
        self.pub.send(b"")

        logger.debug("Joiner :: Exiting")
