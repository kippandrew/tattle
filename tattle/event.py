import asyncio
import time

from tattle import logging

__all__ = [
    'EventManger'
]

LOG = logging.get_logger(__name__)


class EventManager:
    """
    The EventManager is a simple pub/sub style event system based on `PyMitter <https://github.com/riga/pymitter>`_
    """

    __CALLBACK__ = '__callback__'

    def __init__(self, delimiter='.', wildcard='*'):
        self.delimiter = delimiter
        self.wildcard = wildcard
        self.__tree = self.__new_branch()

    def __new_branch(self):
        """
        Returns a new branch.
        """
        return {EventManager.__CALLBACK__: []}

    def __find_branch(self, event):
        """
        Returns a branch of the tree that matches *event*. Wildcards are not applied.
        """
        parts = event.split(self.delimiter)

        if self.__CALLBACK__ in parts:
            return None

        branch = self.__tree
        for p in parts:
            if p not in branch:
                return None
            branch = branch[p]

        return branch

    def __add_listener(self, branch, func, event, ttl):
        """
        Add a listen to a branch
        """
        listeners = branch[self.__CALLBACK__]
        listener = EventListener(func, event, ttl)
        listeners.append(listener)

    def __remove_listener(self, branch, func):
        """
        Removes a listener given by its function from a branch.
        """
        listeners = branch[self.__CALLBACK__]

        indexes = [i for i, l in enumerate(listeners) if l.func == func]
        indexes.reverse()

        for i in indexes:
            listeners.pop(i)

    def on(self, event, func=None, ttl=-1):
        """
        Registers a function to an event.
        """

        def _on(f):
            if not callable(f):
                raise ValueError("%r is not callable.", f)

            parts = event.split(self.delimiter)
            if self.__CALLBACK__ in parts:
                raise ValueError("Event can not contain: %s", self.__CALLBACK__)

            branch = self.__tree
            for p in parts:
                branch = branch.setdefault(p, self.__new_branch())

            self.__add_listener(branch, func, event, ttl)

            return f

        if func is not None:
            return _on(func)
        else:
            return _on

    def once(self, event, func=None):
        """
        Registers a function to an event with *ttl = 1*.
        """
        return self.on(event, func, ttl=1)

    def off(self, event, func=None):
        """
        Removes a function that is registered to an event.
        """

        def _off(f):
            branch = self.__find_branch(event)
            if branch is None:
                return f

            self.__remove_listener(branch, f)

            return f

        if func is not None:
            return _off(func)
        else:
            return _off

    def emit(self, event, *args, **kwargs):
        """
        Emits an event.
        """
        parts = event.split(self.delimiter)
        if self.__CALLBACK__ in parts:
            raise ValueError("Event can not contain: %s", self.__CALLBACK__)

        listeners = self.__tree[self.__CALLBACK__][:]

        branches = [self.__tree]

        # find branches that match
        for p in parts:
            matches = []
            for branch in branches:
                for k, b in branch.items():
                    if k == self.__CALLBACK__:
                        continue
                    if k == p:
                        matches.append(b)
                    elif self.wildcard is not None:
                        if p == self.wildcard or k == self.wildcard:
                            matches.append(b)
            branches = matches

        for b in branches:
            listeners.extend(b[self.__CALLBACK__])

        listeners.sort(key=lambda l: l.time)

        LOG.trace("Emitting event: %s (%d listeners)", event, len(listeners))

        remove = [l for l in listeners if not l(*args, **kwargs)]
        for l in remove:
            self.off(l.event, func=l.func)


class EventListener(object):
    def __init__(self, func, event, ttl):
        super(EventListener, self).__init__()

        self.func = func
        self.event = event
        self.ttl = ttl
        self.time = time.time()
        self._loop = asyncio.get_event_loop()

    def __call__(self, *args, **kwargs):
        """
        Invokes the wrapped function. If the ttl value is non-negative, it is
        decremented by 1. In this case, returns *False* if the ttl value
        approached 0. Returns *True* otherwise.
        """

        # invoke event
        res = self.func(*args, **kwargs)
        if asyncio.coroutines.iscoroutine(res):
            self._loop.create_task(res)

        if self.ttl > 0:
            self.ttl -= 1
            if self.ttl == 0:
                return False

        return True
