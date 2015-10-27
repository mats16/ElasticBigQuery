# -*- coding: utf-8 -*-

from webapp2 import Route
import handlers

_routes = [
    Route(r'/dmp/v1/bqid/get', handlers.GetidHandler, name='get-id'),
    Route(r'/dmp/v1/event/<dataset_id>/<table_id>', handlers.EventHandler, name='event'),
    Route(r'/dmp/task/event/<dataset_id>/<table_id>', handlers.TaskeventHandler, name='task-event'),
]

def get_routes():
    return _routes

def add_routes(app):
    for r in _routes:
        app.router.add(r)
