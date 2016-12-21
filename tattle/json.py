from __future__ import absolute_import

import json
import datetime

from tornado import escape


def _custom_json_handler(obj):
    if isinstance(obj, (datetime.datetime, datetime.time)):
        return obj.isoformat()


def to_json(obj, fp=None):
    if fp is None:
        return json.dumps(obj, default=_custom_json_handler, sort_keys=False)
    else:
        return json.dump(obj, fp, default=_custom_json_handler, sort_keys=False)


def from_json(s):
    return json.loads(escape.to_basestring(s))
