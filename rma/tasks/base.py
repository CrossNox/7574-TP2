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

        logger.info("Ventilate source subscribed to %s", subaddr)
        logger.info("Ventilate syncing to %s", subsyncaddr)
        logger.info("Ventilate source pushing to %s", pushaddr)
        logger.info("Ventilate sync addr %s", syncaddr)
        logger.info("Ventilate looking for sink %s", sinkaddr)

    def run(self):
        logger.info("Syncing with sink")
        self.sink_req.send(b"")
        self.sink_req.recv()

        logger.info("Syncing with all workers")
        subs = 0
        while subs < self.nworkers:
            _ = self.sync_rep.recv()
            self.sync_rep.send(b"")
            subs += 1
            logger.info(f"+1 subscriber ({subs}/{self.nworkers})")

        logger.info("Syncing with pub")
        self.subsync.send(b"")
        self.subsync.recv()

        while True:
            s = self.sub.recv()

            if s == b"":
                break

            self.push.send(s)

        for _ in range(self.nworkers):
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

        logger.info("Ventilate sink :: pulling from %s", pulladdr)
        logger.info("Ventilate sink :: source sync at %s", repaddr)
        logger.info("Ventilate sink :: publishing at %s", pubaddr)
        logger.info("Ventilate sink :: syncing %s at address %s", nsubs, subsyncaddr)

    def run(self):
        logger.info("Syncing with source")
        self.source_rep.recv()
        self.source_rep.send(b"")

        logger.info("Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.info(f"+1 subscriber ({subs}/{self.nworkers})")

        while True:
            s = self.workers_results.recv()

            if s == b"":
                self.nworkers -= 1

                if self.nworkers == 0:
                    self.pub.send(b"")
                    break

            self.pub.send(s)


class VentilatorWorker:
    def __init__(
        self,
        pulladdr,
        reqaddr,
        pushaddr,
        executor_cls,
        executor_kwargs=None,
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

        logger.info("Worker pulling from %s", pulladdr)
        logger.info("Worker synced to %s", reqaddr)
        logger.info("Worker pushing to %s", pushaddr)

    def run(self):
        self.source_req.send(b"")
        self.source_req.recv()
        self.executor.run()
        self.push.send(b"")


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

        logger.info("Worker :: subbed to %s with filter '%s'", subaddr, subfilter)
        logger.info("Worker :: sync with producer at %s", reqaddr)
        logger.info("Worker :: publishing at %s", pubaddr)
        logger.info("Worker :: sync with %s clients at address %s", nsubs, subsyncaddr)

    def _resolve_deps(self, deps):
        logger.info("Worker :: resolving deps")

        final_deps = {}
        _subs = {}

        for dep_name, dep_sync, dep_sub in deps:
            logger.info("%s result: %s sub: %s", dep_name, dep_sync, dep_sub)

            _sub = zmq.Context.instance().socket(zmq.SUB)
            _sub.connect(dep_sub)
            _sub.setsockopt_string(zmq.SUBSCRIBE, "")

            _subs[dep_name] = _sub

            _dep_sync = zmq.Context.instance().socket(zmq.REQ)
            _dep_sync.connect(dep_sync)
            _dep_sync.send(b"")
            _dep_sync.recv()

        logger.info("Worker :: Waiting for dependencies results")

        for dep_name, sub in _subs.items():
            final_deps[dep_name] = json.loads(sub.recv().decode())

        return final_deps

    def run(self):
        logger.info("Worker :: Syncing with producer")
        self.req.send(b"")
        self.req.recv()

        logger.info("Worker :: Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.info(f"Worker :: +1 subscriber ({subs}/{self.nsubs})")

        logger.info("Worker :: Running executor")
        self.executor.run()

        logger.info("Worker :: Sending poison pill")
        self.pub.send(b"")

        logger.info("Worker :: Exiting")
