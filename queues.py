import queue
import threading


class Queues(object):
    q = None
    num_worker_threads = 8
    threads = []
    do_work = None

    def __init__(self, do_work, num_worker_threads=8):
        self.num_worker_threads=num_worker_threads
        self.do_work=do_work

    def worker(self):
        while True:
            queue_item = self.q.get()
            if queue_item is None:
                break
            self.do_work(queue_item)
            self.q.task_done()

    def start(self):
        self.q = queue.Queue()
        self.threads = []
        for i in range(self.num_worker_threads):
            t = threading.Thread(target=self.worker)
            t.start()
            self.threads.append(t)

    def stop(self):
        # stop workers
        for i in range(self.num_worker_threads):
            self.q.put(None)
        for t in self.threads:
            t.join()

    def enqueue(self,queue_item):
        self.q.put(queue_item)

    def join(self):
        self.q.join()


def main():
    def myworker(queue_item):
        print("myworker:"+queue_item)

    queues = Queues(myworker, 8)
    queues.start()
    queues.enqueue("Hello")
    queues.join()
    queues.stop()


if __name__ == "__main__":
    main()

