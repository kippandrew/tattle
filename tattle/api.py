import collections
import traceback

from tornado import web
from tornado import httputil

from tattle import logging
from tattle import json

LOG = logging.get_logger(__name__)


class APIServer(web.Application):
    def __init__(self, cluster):
        # initialize cluster
        self.cluster = cluster

        # initialize handlers
        handlers = [
            (r'/cluster/join?$', JoinAPIHandler),
            (r'/cluster/leave$', LeaveAPIHandler),
            (r'/cluster/nodes/?$', NodeAPIHandler),
            (r'/?.*', DefaultAPIHandler),
        ]

        # initialize super class
        super(APIServer, self).__init__(handlers)

        LOG.debug("Initialized APIServer")


class APIError(web.HTTPError):
    def __init__(self, status_code, error=None, **kwargs):
        self.error = error
        super(APIError, self).__init__(status_code, error, **kwargs)


class APIRequestHandler(web.RequestHandler):
    def initialize(self):
        # call super class
        super(APIRequestHandler, self).initialize()

    def finalize(self):
        # call super class
        super(APIRequestHandler, self).prepare()

    def on_finish(self):
        super(APIRequestHandler, self).on_finish()
        self.finalize()

    @property
    def cluster(self):
        return self.application.cluster

    def get_json(self, key=None, default=None):
        if not hasattr(self, '_json'):
            self._parse_json()
        if key is not None:
            return self._json.get(key, default)
        return self._json

    def _parse_json(self):
        # parse request json
        if 'Content-Type' in self.request.headers:
            content_type, content_type_options = httputil._parse_header(self.request.headers['Content-Type'])
            if content_type == 'application/json':
                try:
                    # parse JSON
                    self._json = json.from_json(self.request.body)
                except ValueError as e:
                    LOG.warn("Unable to parse json: %s" % e)
                    self._json = dict()
            else:
                self._json = dict()
        else:
            self._json = dict()

    def write_error(self, status_code, **kwargs):
        exc_info = kwargs.pop('exc_info', None)
        if exc_info is not None:
            return self._write_exception_json(status_code, exc_info, **kwargs)
        else:
            return self._write_error_json(status_code, **kwargs)

    def _write_error_json(self, status_code, **kwargs):
        error = dict()
        error.update(**kwargs)
        error['error'] = self._reason
        self.finish(error)

    def _write_exception_json(self, status_code, exc_info, **kwargs):
        ex_type, ex, ex_traceback = exc_info

        error = dict()
        if isinstance(ex, APIError):

            # get error message
            error['error'] = (ex.error if hasattr(ex, 'error') else self._reason) or self._reason

            # include kwargs if any
            if hasattr(ex, 'kwargs'):
                error.update(ex.kwargs)

            # log error
            LOG.error(ex)
        else:
            error['error'] = self._reason

            # if debug is enabled, include traceback
            if self.settings.get('debug', False):
                error['traceback'] = [t for t in traceback.format_exception(*exc_info)]

                # log exception
                LOG.error("Unhandled exception: ", exc_info=1)

        # include any kwargs that may of come through
        error.update(kwargs)
        self.finish(error)


# noinspection PyAbstractClass
class DefaultAPIHandler(APIRequestHandler):
    def prepare(self):
        raise APIError(404)


# noinspection PyAbstractClass
class JoinAPIHandler(APIRequestHandler):
    def post(self):
        nodes = self.get_json('nodes', None)
        if nodes is None or not isinstance(nodes, collections.Sequence):
            raise APIError(400, error="A list of nodes to join must be specified.")


# noinspection PyAbstractClass
class LeaveAPIHandler(APIRequestHandler):
    def post(self):
        pass


# noinspection PyAbstractClass
class NodeAPIHandler(APIRequestHandler):
    pass
