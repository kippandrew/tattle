import asyncio
import traceback
import sys

from aiohttp import web

from tattle import logging
from tattle import json

__all__ = [
    'APIServer',
    'start_server',
    'stop_server',
]

LOG = logging.get_logger(__name__)


class APIError(web.HTTPError):
    def __init__(self, status_code, message=None):
        self.status_code = status_code
        self.message = message
        super(APIError, self).__init__()


def error_middleware():
    # noinspection PyUnusedLocal
    @asyncio.coroutine
    def _middleware(app, handler):

        def _write_exception_json(status_code=500, exc_info=None):
            if exc_info is None:
                exc_info = sys.exc_info()
            exception = exc_info[2]
            error = {'error': "Internal Server Error"}
            error['traceback'] = [t for t in traceback.format_exception(*exc_info)]
            return web.Response(status=status_code,
                                body=json.to_json(error).encode('utf-8'),
                                content_type='application/json')

        def _write_error_json(status_code, message=None):
            return web.Response(status=status_code,
                                body=json.to_json({'error': message}).encode('utf-8'),
                                content_type='application/json')

        @asyncio.coroutine
        def _middleware_handler(request):
            try:
                response = yield from handler(request)
                return response
            except APIError as ex:
                return _write_error_json(ex.status_code, ex.message or ex.reason)
            except web.HTTPError as ex:
                return _write_error_json(ex.status_code, ex.reason)
            except Exception as ex:
                return _write_exception_json()

        return _middleware_handler

    return _middleware


class APIServer(web.Application):
    def __init__(self, cluster):
        """
        Initialize instance of the APIServer class
        :param cluster:
        """

        # initialize cluster
        self.cluster = cluster

        # initialize super class
        super(APIServer, self).__init__(middlewares=[error_middleware()])

        # initialize routes
        self.router.add_route('*', '/cluster/join', JoinAPIHandler)
        self.router.add_route('*', '/cluster/leave', LeaveAPIHandler)
        self.router.add_route('*', '/cluster/members/', MemberAPIHandler)

        LOG.debug("Initialized APIServer")


class APIRequestHandler(web.View):
    @property
    def cluster(self):
        return self.request.app.cluster


class JoinAPIHandler(APIRequestHandler):
    @asyncio.coroutine
    def post(self):
        pass


class LeaveAPIHandler(APIRequestHandler):
    @asyncio.coroutine
    def post(self):
        pass


class MemberAPIHandler(APIRequestHandler):
    @asyncio.coroutine
    def get(self):
        return web.json_response(dict(members=self.cluster.members))


def start_server(app, port, host='127.0.0.1'):
    loop = app.loop

    # create handler
    handler = app.make_handler()

    # signal app startup
    loop.run_until_complete(app.startup())

    # create socket server
    server = loop.run_until_complete(loop.create_server(handler, host, port, ssl=None, backlog=100))
    LOG.info("Started API server on %s:%d", host, port)

    return (handler, server)


def stop_server(app, server, handler, timeout=10):
    loop = app.loop

    # close server socket
    server.close()
    loop.run_until_complete(server.wait_closed())

    # signal app shutdown
    loop.run_until_complete(app.shutdown())

    # shutdown handler
    loop.run_until_complete(handler.shutdown(timeout))

    # signal app cleanup
    loop.run_until_complete(app.cleanup())
