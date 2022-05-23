import zmq

from rma.utils import get_logger

logger = get_logger(__name__)


class VentilatorSource:
    def __init__(
        self, subaddr, pushaddr, syncaddr, sinkaddr, nsubs: int, subfilter: str = "",
    ):
        self.context = zmq.Context.instance()  # type: ignore

        # SUB where to get data from
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect(subaddr)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, subfilter)

        # PUSH where to publish data
        self.push = self.context.socket(zmq.PUSH)
        self.push.bind(pushaddr)

        # REP to sync workers
        self.sync_rep = self.context.socket(zmq.REP)
        self.sync_rep.bind(syncaddr)

        # Number of workers to coordinate
        self.nsubs = nsubs

        # REQ to sync with sink
        self.sink_req = self.context.socket(zmq.REQ)
        self.sink_req.connect(sinkaddr)

    def run(self):
        self.sink_req.send(b"")
        self.sink_req.recv()

        subs = 0
        while subs < self.nsubs:
            _ = self.sync_rep.recv()
            self.sync_rep.send(b"")
            subs += 1
            logger.info(f"+1 subscriber ({subs}/{self.nsubs})")

        while True:
            s = self.sub.recv()

            if s == b"":
                break

            self.push.send(s)


class VentilatorSink:
    def __init__(self, pulladdr, repaddr, pubaddr, nworkers: int):
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

        # Number of workers to keep track of exits
        self.nworkers = nworkers

    def run(self):
        self.source_rep.recv()
        self.source_rep.send(b"")

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
        context=None,
    ):
        self.context = context or zmq.Context()

        # PULL address to get message from
        self.task_pull = self.context.socket(zmq.PULL)
        self.task_pull.bind(pulladdr)

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

    def run(self):
        self.executor.run()
