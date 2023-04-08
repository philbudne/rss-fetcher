"""
"direct drive" (no queue) worker subprocess management

No queue so that scheduler can have absolute knowledge of what work is
being done, and avoid conflicts.

And unlike multiprocessing process pool which "applies" a function to
a bounded list of work items (and like queued work scenarios like
celery and rq), the work stream is open-ended.

Manager is written as a class for encapsulation/extension,
it is not currenly possible to have two active Manager objects
(depends on exclusive use/access to SIGALRM)
"""

import json
import os
import select
import signal
import socket
import sys
from types import FrameType
from typing import Any, Dict, Optional, Tuple


MAXJSON = 32 * 1024
TIMEOUT = 30.0

class JobTimeoutException(Exception):
    """class for job timeout exception"""


class Worker:
    """
    must subclass with additional methods for work.

    WISH: have an async decorator for the work methods that
        performs a "call" and returns the result.
    """
    def __init__(self, n: int, timeout: float = TIMEOUT):
        """
        forks child process (running infinite loop),
        returns in parent process only.
        """
        # XXX could supply manager with a Worker subclass, instead of a func
        #       make timeout a class var?
        # XXX wrap in try??

        self.n = n

        # single bidirectional socketpair instead of two pipes:
        psock, csock = socket.socketpair()  # parent, child
        self.pid = os.fork()
        if self.pid == 0:       # child/worker
            psock.close()       # close parent/manager end
            self._child(csock, timeout)
            # should not be reached!
        else:                   # manager/parent
            csock.close()       # close child/worker end
            self.sock = psock
            self.wactive = False

    def _child(self, csock, timeout) -> None:
        """
        child: loop reading method & args, returning result.
        called from __init__, and never returns.
        """
        # XXX close log file, and open new one based on "self.n"
        #   (basing file name on pid would mean pruning of
        #   files from a previous run would not occur)???

        def alarm_handler(sig: int, frame: Optional[FrameType]) -> None:
            raise JobTimeoutException()
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        while True:
            try:
                msg = csock.recv(MAXJSON)
            except ConnectionResetError:  # remote fully closed?
                break
            if not msg:
                break       # EOF
            method_name, args, kw = json.loads(msg)  # see Manager.send
            ret = {'method': method_name,
                   'args': args,
                   'kw': kw}

            try:
                if timeout:
                    self.set_job_timeout(timeout)
                # will raise AttributeError for unknown method_name
                method = getattr(self, method_name)
                ret['ret'] = method(*args, **kw)
            except Exception as e:
                ret['exc'] = type(e).__name__
                ret['info'] = str(e)

            if timeout:
                self.set_job_timeout()

            # XXX do json.dumps under separate try (so can report error!)?

            try:
                csock.send(json.dumps(ret).encode('utf8'))
            except BrokenPipeError:  # remote closed for read
                break
            except TypeError:
                break
        sys.exit(0)

    def set_job_timeout(self, sec=0) -> None:
        """
        for use in child process ONLY!
        may be called by used defined methods!!
        by default clears timeout.
        """
        signal.setitimer(signal.ITIMER_REAL, sec)

    def shut_wr(self) -> None:
        if self.sock:
            self.sock.shutdown(socket.SHUT_WR)

    def close(self) -> None:
        if self.sock:
            self.sock.close()

    def fileno(self) -> int:
        """return file desciptor number for read poll/select"""
        return self.sock.fileno()

    # XXX add _timeout=TIMEOUT argument???
    def call(self, method_name: str, *args: Any, **kw: Any) -> None:
        assert not self.wactive
        # XXX verify method_name exists w/ hasattr(self, name)???
        msg = json.dumps([method_name, args, kw])
        # XXX wrap in try?
        self.sock.send(msg.encode('utf8'))
        self.wactive = True

    def recv(self) -> Tuple[bool, Any]:
        """
        call ONLY after a "call" to wait for result
        """
        assert self.wactive
        # XXX use buffered I/O?
        msg = self.sock.recv(8192)
        if msg:
            print(self.fileno(), '->', msg)
            self.wactive = False
            return True, json.loads(msg)
        # XXX mark as closed
        return False, None

    def wait(self) -> int:
        """wait for child process (after reading EOF)"""
        pid, status = os.waitpid(self.pid, os.WNOHANG)
        return status

class Manager:
    """
    manager for subprocess workers
    encapsulated for reuse
    depends on exclusive use of SIGALRM.
    (cannot currently have multiple Managers at same time)
    """

    def __init__(self,
                 nworkers: int,
                 worker_class: type[Worker],
                 timeout: float = TIMEOUT):
        self.nworkers = nworkers  # desired number of workers
        self.worker_class = worker_class
        self.timeout = timeout

        self.cworkers = 0         # current number of workers
        self.active = 0           # workers w/ work
        self.worker_by_fd: Dict[int, Worker] = {}

        for i in range(0, nworkers):
            self._create_worker(i)

    def _create_worker(self, n: int) -> Worker:
        w = self.worker_class(n, self.timeout)
        self.worker_by_fd[w.fileno()] = w
        self.cworkers += 1
        return w

    def poll(self, timeout: Optional[float] = None) -> None:
        r, w_, x_ = select.select(self.worker_by_fd.keys(), [], [], timeout)
        print("r:", r)
        for fd in r:
            w: Worker = self.worker_by_fd[fd]
            ok, ret = w.recv()
            self.active -= 1
            if ok:
                if (m := ret.get('method')):
                    # if Worker <method>_done method exists,
                    # call it (in Manager process)
                    if (done := getattr(w, m + '_done', None)):
                        done(ret)
                w.wactive = False
            else:               # saw EOF (from child exit)
                n = w.n
                w.close()
                del w
                # XXX log???
                self._create_worker(n)  # (unless shutting down)!!

    def find_available_worker(self) -> Optional[Worker]:
        if self.active == self.nworkers:
            return None
        for w in self.worker_by_fd.values():
            if not w.wactive:
                return w
        # XXX should not happen
        return None

    def close_all(self, timeout: Optional[float] = None) -> None:
        for w in self.worker_by_fd.values():
            w.shut_wr()         # close for write
        self.poll(timeout)


if __name__ == '__main__':
    import time

    class TestWorker(Worker):
        def f(self, x: int) -> None:
            print("f:", x)
            time.sleep(0.5)

        def f_done(self, x: Dict[str,Any]) -> None:
            print("f_done:", x)

    m = Manager(2, TestWorker)
    i = 0
    while True:
        while w := m.find_available_worker():
            w.call("f", i)
            i += 1
        if i >= 10:
            break
        m.poll()
    m.close_all(0.5)