from rma.tasks.base import Sink


class PrintSink(Sink):
    def sink(self, msg):
        print(msg)
