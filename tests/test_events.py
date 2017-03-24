import unittest
import unittest.mock

import asynctest
import asynctest.mock

from tattle import event


class EventManagerTestCase(unittest.TestCase):
    def setUp(self):
        # setup super class
        super(EventManagerTestCase, self).setUp()

        self.events = event.EventManager()

    def test_emit_on(self):
        on_some_event = unittest.mock.Mock()
        self.events.on('some.thing.happened', on_some_event)
        self.events.emit('some.thing.happened', 'foo', derp='bar')
        self.events.emit('some.thing.happened', 'foo', derp='bar')
        on_some_event.assert_called_with('foo', derp='bar')
        self.assertEqual(on_some_event.call_count, 2)

    def test_emit_once(self):
        on_some_event = unittest.mock.Mock()
        self.events.once('some.thing.happened', on_some_event)
        self.events.emit('some.thing.happened', 'foo', derp='bar')
        self.events.emit('some.thing.happened', 'foo', derp='bar')
        on_some_event.assert_called_with('foo', derp='bar')
        self.assertEqual(on_some_event.call_count, 1)

    def test_emit_off(self):
        on_some_event = unittest.mock.Mock()
        self.events.on('some.thing.happened', on_some_event)
        self.events.emit('some.thing.happened', 'foo', derp='bar')
        on_some_event.assert_called_with('foo', derp='bar')
        self.events.off('some.thing.happened', on_some_event)
        self.events.emit('some.thing.happened', 'foo', derp='bar')
        self.assertEqual(on_some_event.call_count, 1)

    def test_emit_wildcard(self):
        on_some_event = unittest.mock.Mock()
        self.events.on('some.thing.happened', on_some_event)
        self.events.emit('some.thing.*', 'foo', derp='bar')
        on_some_event.assert_called_with('foo', derp='bar')
