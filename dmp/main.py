# -*- coding: utf-8 -*-

from google.appengine.ext import vendor
vendor.add('dmp/external')

import webapp2
import routes

app = webapp2.WSGIApplication(debug=False)

routes.add_routes(app)
