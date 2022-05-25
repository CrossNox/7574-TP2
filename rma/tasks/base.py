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
        self.sub.sndhwm = 0
        self.sub.rcvhwm = 0

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
        # First, we need to know that all workers are running and listening
        # Then, we let the sink know we are going to start processing stuff
        # so they should listen to results.
        # Now we are all ready to get incoming messages and fan them out.
        # At some point, we get a poison pill. We send a poison pill for each
        # worker. Each will consume one and consume no more messages, leaving.

        logger.debug("Syncing with all workers")
        subs = 0
        while subs < self.nworkers:
            _ = self.sync_rep.recv()
            self.sync_rep.send(b"")
            subs += 1
            logger.debug(f"VentilatorSource :: +1 subscriber ({subs}/{self.nworkers})")

        logger.debug("VentilatorSource :: Syncing with sink")
        self.sink_req.send(b"")
        self.sink_req.recv()

        logger.debug("Syncing with pub")
        self.subsync.send(b"")
        self.subsync.recv()

        while True:
            s = self.sub.recv()

            if s == b"":
                break

            self.push.send(s)

        logger.debug("ACKing poison pill")
        self.subsync.send(b"")
        self.subsync.recv()

        logger.debug("Sending %s poison pills to workers", self.nworkers)
        pill_acks = 0
        self.sync_rep.rcvtimeo = 1000
        while pill_acks < self.nworkers:
            self.push.send(b"")
            try:
                self.sync_rep.recv()
                self.sync_rep.send(b"")
                pill_acks += 1
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
                else:
                    raise

        logger.debug("Expecting poison pill ack from sink")
        self.sink_req.send(b"")
        self.sink_req.recv()

        logger.debug("VentilatorSource :: exiting")


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
        logger.info("Sync with ventilator source")
        self.source_req.send(b"")
        self.source_req.recv()

        self.executor.run()

        logger.info("Acking poison pill to ventilator source")
        self.source_req.send(b"")
        self.source_req.recv()

        logger.info("Sending poison pill to ventilator sink")
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
        self.pub.sndhwm = 0
        self.pub.rcvhwm = 0
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
        logger.debug("Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.debug(f"Ventilate sink :: +1 subscriber ({subs}/{self.nworkers})")

        logger.debug("Syncing with source")
        self.source_rep.recv()
        self.source_rep.send(b"")

        alive_workers = self.nworkers
        while alive_workers > 0:
            s = self.workers_results.recv()

            if s == b"":
                alive_workers -= 1
            else:
                self.pub.send(s)

        logger.debug("ACKing poison pill with source")
        self.source_rep.recv()
        self.source_rep.send(b"")

        logger.debug("VentilatorSink :: Sending poison pill")

        pill_acks = 0
        self.syncsubs.rcvtimeo = 1000
        while pill_acks < self.nsubs:
            self.pub.send(b"")
            try:
                self.syncsubs.recv()
                self.syncsubs.send(b"")
                pill_acks += 1
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
                else:
                    raise

        logger.debug("VentilatorSink :: Exiting")


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
        self.sub.sndhwm = 0
        self.sub.rcvhwm = 0

        # REQ to sync with producer
        self.req = self.context.socket(zmq.REQ)
        self.req.connect(reqaddr)

        # PUB to publish transformed data to
        self.pub = self.context.socket(zmq.PUB)
        self.pub.sndhwm = 0
        self.pub.rcvhwm = 0
        self.pub.bind(pubaddr)

        # REP to sync with subscribers
        self.syncsubs = self.context.socket(zmq.REP)
        self.syncsubs.bind(subsyncaddr)

        self.nsubs = nsubs

        self.deps = deps or []
        self.executor_cls = executor_cls
        self.executor_kwargs = executor_kwargs or {}

        logger.debug("Worker :: subbed to %s with filter '%s'", subaddr, subfilter)
        logger.debug("Worker :: sync with producer at %s", reqaddr)
        logger.debug("Worker :: publishing at %s", pubaddr)
        logger.debug("Worker :: sync with %s clients at address %s", nsubs, subsyncaddr)

    def run(self):
        logger.debug("Worker :: Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.debug(f"Worker :: +1 subscriber ({subs}/{self.nsubs})")

        logger.debug("Worker :: Syncing with producer")
        self.req.send(b"")
        self.req.recv()

        logger.debug("Worker :: resolving %s dependencies", len(self.deps))
        final_deps = {}
        # _deps_subs = {}

        for dep_name, dep_sync, dep_sub in self.deps:
            logger.debug("Worker :: %s result: %s sub: %s", dep_name, dep_sync, dep_sub)

            _sub = zmq.Context.instance().socket(zmq.SUB)
            _sub.connect(dep_sub)
            _sub.setsockopt_string(zmq.SUBSCRIBE, "")
            _sub.sndhwm = 0
            _sub.rcvhwm = 0

            # Let's keep it simple: deps can only return scalars
            # _deps_subs[dep_name] = _sub

            _dep_sync = zmq.Context.instance().socket(zmq.REQ)
            _dep_sync.connect(dep_sync)
            _dep_sync.send(b"")
            _dep_sync.recv()

            final_deps[dep_name] = json.loads(_sub.recv().decode())

            while _sub.recv() != b"":
                pass

            _dep_sync.send(b"")
            _dep_sync.recv()

        # for dep_name, sub in _deps_subs.items():
        #    final_deps[dep_name] = json.loads(sub.recv().decode())

        logger.debug("Worker :: Running executor")
        # Thingy to execute
        executor = self.executor_cls(
            **final_deps, task_in=self.sub, task_out=self.pub, **self.executor_kwargs
        )

        executor.run()

        logger.debug("ACKing poison pill")
        self.req.send(b"")
        self.req.recv()

        logger.debug("Worker :: Sending poison pill")
        pill_acks = 0
        self.syncsubs.rcvtimeo = 1000
        while pill_acks < self.nsubs:
            self.pub.send(b"")
            try:
                self.syncsubs.recv()
                self.syncsubs.send(b"")
                pill_acks += 1
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
                else:
                    raise

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
        self.pub.sndhwm = 0
        self.pub.rcvhwm = 0
        self.pub.bind(pubaddr)

        # REP to sync with subscribers
        self.syncsubs = self.context.socket(zmq.REP)
        self.syncsubs.bind(repaddr)
        self.nsubs = nsubs

        # Subscriptions
        self.sub = self.context.socket(zmq.SUB)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sub.sndhwm = 0
        self.sub.rcvhwm = 0

        self.inputs = inputs

        self.req_sckt = self.context.socket(zmq.REQ)
        for dep_sync, dep_sub in self.inputs:
            self.sub.connect(dep_sub)
            self.req_sckt.connect(dep_sync)

        if executor_kwargs is None:
            executor_kwargs = dict()

        self.executor = executor_cls(
            task_in=self.sub,
            task_out=self.pub,
            req_sckt=self.req_sckt,
            **executor_kwargs,
        )

    def run(self):
        logger.debug("Joiner :: Syncing with all subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncsubs.recv()
            self.syncsubs.send(b"")
            subs += 1
            logger.debug(f"Joiner :: +1 subscriber ({subs}/{self.nsubs})")

        logger.debug("Joiner :: Syncing with all inputs")

        for _ in range(len(self.inputs)):
            self.req_sckt.send(b"")
            self.req_sckt.recv()

        logger.debug("Joiner :: Running executor")
        self.executor.run()

        logger.debug("Joiner :: Sending poison pill")

        pill_acks = 0
        self.syncsubs.rcvtimeo = 1000
        while pill_acks < self.nsubs:
            self.pub.send(b"")
            try:
                self.syncsubs.recv()
                self.syncsubs.send(b"")
                pill_acks += 1
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
                else:
                    raise

        logger.debug("Joiner :: Exiting")
