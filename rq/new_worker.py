import sys
import os
import datetime
import errno
import random
import time
try:
    from procname import setprocname
except ImportError:
    def setprocname(*args, **kwargs):  # noqa
        pass
import socket
import signal
import traceback
from cPickle import dumps

from multiprocessing import Semaphore, Array, Pool

try:
    from logbook import Logger
    Logger = Logger   # Does nothing except it shuts up pyflakes annoying error
except ImportError:
    from logging import Logger
from .queue import Queue, get_failed_queue
from .connections import get_current_connection
from .job import Status
from .utils import make_colorizer
from .exceptions import NoQueueError, UnpickleError
from .timeouts import death_penalty_after
from .version import VERSION

green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')
blue = make_colorizer('darkblue')

DEFAULT_RESULT_TTL = 500

class StopRequested(Exception):
    pass


def iterable(x):
    return hasattr(x, '__iter__')


def compact(l):
    return [x for x in l if x is not None]

_signames = dict((getattr(signal, signame), signame) \
                    for signame in dir(signal) \
                    if signame.startswith('SIG') and '_' not in signame)


def signal_name(signum):
    # Hackety-hack-hack: is there really no better way to reverse lookup the
    # signal name?  If you read this and know a way: please provide a patch :)
    try:
        return _signames[signum]
    except KeyError:
        return 'SIG_UNKNOWN'


class BaseWorker(object):

    redis_worker_namespace_prefix = 'rq:worker:'
    redis_workers_keys = 'rq:workers'

    @classmethod
    def all(cls, connection=None):
        """Returns an iterable of all Workers.
        """
        if connection is None:
            connection = get_current_connection()
        reported_working = connection.smembers(cls.redis_workers_keys)
        workers = [cls.find_by_key(key, connection) for key in
                reported_working]
        return compact(workers)

    @classmethod
    def find_by_key(cls, worker_key, connection=None):
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.
        """
        prefix = cls.redis_worker_namespace_prefix
        name = worker_key[len(prefix):]
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % (worker_key,))

        if connection is None:
            connection = get_current_connection()
        if not connection.exists(worker_key):
            return None

        name = worker_key[len(prefix):]
        worker = cls([], name, connection=connection)
        queues = connection.hget(worker.key, 'queues')
        worker._state = connection.hget(worker.key, 'state') or '?'
        if queues:
            worker.queues = [Queue(queue, connection=connection)
                                for queue in queues.split(',')]
        return worker


    def __init__(self, queues, name=None, default_result_ttl=DEFAULT_RESULT_TTL,
            connection=None, exc_handler=None):  # noqa
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        if isinstance(queues, Queue):
            queues = [queues]
        self._name = name
        self.queues = queues
        self.validate_queues()
        self._exc_handlers = []
        self.default_result_ttl = default_result_ttl
        self._state = 'starting'
        self._stopped = False
        self.log = Logger('worker')
        self.failed_queue = get_failed_queue(connection=self.connection)

        # By default, push the "move-to-failed-queue" exception handler onto
        # the stack
        self.push_exc_handler(self.move_to_failed_queue)
        if exc_handler is not None:
            self.push_exc_handler(exc_handler)


    def validate_queues(self):  # noqa
        """Sanity check for the given queues."""
        if not iterable(self.queues):
            raise ValueError('Argument queues not iterable.')
        for queue in self.queues:
            if not isinstance(queue, Queue):
                raise NoQueueError('Give each worker at least one Queue.')

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return map(lambda q: q.name, self.queues)

    def queue_keys(self):
        """Returns the Redis keys representing this worker's queues."""
        return map(lambda q: q.key, self.queues)


    @property  # noqa
    def name(self):
        """Returns the name of the worker, under which it is registered to the
        monitoring system.

        By default, the name of the worker is constructed from the current
        (short) host name and the current PID.
        """
        if self._name is None:
            hostname = socket.gethostname()
            shortname, _, _ = hostname.partition('.')
            self._name = '%s.%s' % (shortname, self.pid)
        return self._name

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return self.redis_worker_namespace_prefix + self.name

    @property
    def pid(self):
        """The current process ID."""
        return os.getpid()

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        setprocname('rq: %s' % (message,))


    def register_birth(self):  # noqa
        """Registers its own birth."""
        self.log.debug('Registering birth of worker %s' % (self.name,))
        if self.connection.exists(self.key) and \
                not self.connection.hexists(self.key, 'death'):
            raise ValueError(
                    'There exists an active worker named \'%s\' '
                    'already.' % (self.name,))
        key = self.key
        now = time.time()
        queues = ','.join(self.queue_names())
        with self.connection.pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', now)
            p.hset(key, 'queues', queues)
            p.sadd(self.redis_workers_keys, key)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with self.connection.pipeline() as p:
            # We cannot use self.state = 'dead' here, because that would
            # rollback the pipeline
            p.srem(self.redis_workers_keys, self.key)
            p.hset(self.key, 'death', time.time())
            p.expire(self.key, 60)
            p.execute()

    def set_state(self, new_state):
        self._state = new_state
        self.connection.hset(self.key, 'state', new_state)

    def get_state(self):
        return self._state

    state = property(get_state, set_state)

    @property
    def stopped(self):
        return self._stopped

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        def request_force_stop(signum, frame):
            """Terminates the application (cold shutdown).
            """
            print 'PID IS %s' % os.getpid()
            self.log.warning('Cold shut down.')
            # Need to call ``handle_cold_shutdown`` implemented by subclasses
            self.handle_cold_shutdown()
            raise SystemExit()

        def request_stop(signum, frame):
            """Stops the current worker loop but waits for child processes to
            end gracefully (warm shutdown).
            """
            print 'PID IS %s' % os.getpid()
            self.log.debug('Got signal %s.' % signal_name(signum))

            signal.signal(signal.SIGINT, request_force_stop)
            signal.signal(signal.SIGTERM, request_force_stop)

            msg = 'Warm shut down requested.'
            self.log.warning(msg)

            # If shutdown is requested in the middle of a job, wait until
            # finish before shutting down
            if self.is_busy():
                
                self._stopped = True
                self.log.debug('Stopping after current horse is finished. '
                               'Press Ctrl+C again for a cold shutdown.')
            else:
                raise StopRequested()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    def is_busy(self):
        # Each worker class has to implement a way of checking whether it is
        # in the middle of running one or more jobs
        raise NotImplementedError('Implement this in a subclass.')
    
    def handle_exception(self, job, *exc_info):
        """Walks the exception handler stack to delegate exception handling."""
        exc_string = ''.join(
                traceback.format_exception_only(*exc_info[:2]) +
                traceback.format_exception(*exc_info))
        self.log.error(exc_string)

        for handler in reversed(self._exc_handlers):
            self.log.debug('Invoking exception handler %s' % (handler,))
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    def move_to_failed_queue(self, job, *exc_info):
        """Default exception handler: move the job to the failed queue."""
        exc_string = ''.join(traceback.format_exception(*exc_info))
        self.log.warning('Moving job to %s queue.' % self.failed_queue.name)
        self.failed_queue.quarantine(job, exc_info=exc_string)

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()

    def work(self, burst=False):
        self._install_signal_handlers()
        did_perform_work = False
        self.register_birth()
        self.log.info('RQ worker started, version %s' % VERSION)
        self.state = 'starting'
        
        try:
            while True:
                if self.stopped:
                    self.log.info('Stopping on request.')
                    break

                qnames = self.queue_names()
                self.procline('Listening on %s' % ','.join(qnames))
                self.log.info('')
                self.log.info('*** Listening on %s...' % \
                        green(', '.join(qnames)))
                wait_for_job = not burst
                try:
                    result = Queue.dequeue_any(self.queues, wait_for_job, \
                            connection=self.connection)
                    if result is None:
                        break
                except StopRequested:
                    break
                except UnpickleError as e:
                    msg = '*** Ignoring unpickleable data on %s.' % \
                            green(e.queue.name)
                    self.log.warning(msg)
                    self.log.debug('Data follows:')
                    self.log.debug(e.raw_data)
                    self.log.debug('End of unreadable data.')
                    self.failed_queue.push_job_id(e.job_id)
                    continue

                job, queue = result
                # Use the public setter here, to immediately update Redis
                job.status = Status.STARTED
                self.log.info('%s: %s (%s)' % (green(queue.name),
                    blue(job.description), job.id))

                self.spawn_child()

                did_perform_work = True

        finally:
            self.register_death()
        
        return did_perform_work

    def spawn_child(self):
        raise NotImplementedError('Implement this in a subclass.')

    def handle_cold_shutdown(self):
        raise NotImplementedError('Implement this in a subclass.')

    def fake_work(self):
        sleep_time = 3 * random.random()
        print datetime.datetime.now(), '- Hello from', os.getpid(), '- %.3fs' % sleep_time
        time.sleep(5)


class ForkingWorker(BaseWorker):

    def __init__(self, num_processes=1, *args, **kwargs):
        # Set up sync primitives, to communicate with the spawned children
        self._semaphore = Semaphore(num_processes)
        self._slots = Array('i', [0] * num_processes)
        super(ForkingWorker, self).__init__(*args, **kwargs)

    def is_busy(self):
        # If any of the worker slot is non zero, that means there's a job still running
        return any(self._slots)

    def handle_cold_shutdown(self):

        for pid in self._slots:
            if pid:
                msg = 'Taking down horse %d with me.' % pid
                self.log.debug(msg)
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError as e:
                    # ESRCH ("No such process") is fine with us
                    if e.errno != errno.ESRCH:
                        self.log.debug('Horse already down.')
                        raise

    def spawn_child(self):
        """Forks and executes the job."""
        self._semaphore.acquire()    # responsible for the blocking

        # Select an empty slot from self._slots (the first 0 value is picked)
        # The implementation guarantees there will always be at least one empty slot
        for slot, value in enumerate(self._slots):
            if value == 0:
                break

        # The usual hardcore forking action
        child_pid = os.fork()
        if child_pid == 0:
            random.seed()
            # Within child
            try:
                self.fake_work()
            finally:
                # This is the new stuff.  Remember, we're in the child process
                # currently. When all work is done here, free up the current
                # slot (by writing a 0 in the slot position).  This
                # communicates to the parent that the current child has died
                # (so can safely be forgotten about).
                self._slots[slot] = 0
                self._semaphore.release()
                if self._stopped:
                    print 'STOP REQUESTED'
                os._exit(0)
                
        else:
            # Within parent, keep track of the new child by writing its PID
            # into the first free slot index.
            self._slots[slot] = child_pid