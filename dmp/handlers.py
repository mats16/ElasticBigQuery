# -*- coding: utf-8 -*-

import logging
import time
import datetime
import urllib2
import json
import base64
import uuid
import woothee
from webapp2 import RequestHandler
from google.appengine.api import app_identity
from google.appengine.api.labs import taskqueue
from lib.bigquery import *

PROJECT_ID = app_identity.get_application_id()
DEFAULT_HEADERS = [
    ("Access-Control-Allow-Headers", "X-Requested-With, X-TD-Write-Key, Content-Type"),
    ("Access-Control-Allow-Methods", "GET, POST"),
    ("Access-Control-Allow-Origin", "*"),
]
GIF_PIXEL = base64.b64decode("R0lGODlhAQABAIAAAP///////yH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")

def generate_bqid(cookies):
    if u"bqid" in cookies and len(cookies.get(u"bqid")) == 32:
        bqid = cookies.get(u"bqid")
    else:
        bqid = unicode(str(uuid.uuid4()).replace("-",""))
    return bqid

def modify_row_beacon(request):
    ua = request.headers.get(u"User-Agent")
    ua_parse = woothee.parse(ua)
    headers = request.headers
    params = request.params
    row = {}
    for key in params.keys():
        row[key] = params[key]
    row.update({
        u"time":                time.time(),
        u"bqid":                generate_bqid(request.cookies),
        u"referrer":            headers.get(u"Referer"),
        u"td_ip":               request.remote_addr,
        u"td_browser":          ua_parse.get(u"name"),
        u"td_browser_version":  ua_parse.get(u"version"),
        u"td_os":               ua_parse.get(u"os"),
        u"td_os_version":       ua_parse.get(u"os_version"),
        u"user_agent":          ua,
        u"x_country":           headers.get(u"X-Appengine-Country"),
        u"x_region":            headers.get(u"X-Appengine-Region"),
        u"x_city":              headers.get(u"X-Appengine-City"),
        u"x_citylatlong":       headers.get(u"X-Appengine-Citylatlong"),
        })
    return row

def modify_row_td(request):
    ua = request.headers.get(u"User-Agent")
    ua_parse = woothee.parse(ua)
    headers = request.headers
    modified = str(request.params.get(u"modified"))
    row = json.loads(base64.b64decode(request.params.get(u"data")))
    for i in [u"td_path", u"td_referrer", u"td_url"]:
        if ( row.get(i) ):
            row[i] = urllib2.unquote(row[i].encode('utf8'))
    row.update({
        u"time":                float(".".join([modified[0:10], modified[10:13]])),
        u"bqid":                generate_bqid(request.cookies),
        u"td_ip":               request.remote_addr,
        u"td_browser":          ua_parse.get(u"name"),
        u"td_browser_version":  ua_parse.get(u"version"),
        u"td_os":               ua_parse.get(u"os"),
        u"td_os_version":       ua_parse.get(u"os_version"),
        u"user_agent":          ua,
        u"x_country":           headers.get(u"X-Appengine-Country"),
        u"x_region":            headers.get(u"X-Appengine-Region"),
        u"x_city":              headers.get(u"X-Appengine-City"),
        u"x_citylatlong":       headers.get(u"X-Appengine-Citylatlong"),
        })
    return row

def modify_schema(row):
    schema = [{ u"name": u"time", u"type": u"TIMESTAMP", u"mode": u"REQUIRED" }]
    fields = row.keys()
    fields.sort()
    for field in fields:
        if field != u"time":
            schema.append({ u"name": field, u"type": u"STRING", u"mode": u"NULLABLE" })
    return schema


class GetidHandler(RequestHandler):

    def get(self):
        bqid = generate_bqid(self.request.cookies)
        callback = str(self.request.params.get("callback"))
        self.response.body = "{{\"bqid\": \"{0}\"}}".format(str(bqid))
        if not callback == "None":
            self.response.body = "{0}({1})".format(callback, self.response.body)
        self.response.content_type = "application/json"
        self.response.set_cookie("bqid", value=bqid, max_age=60*60*24*365*1, path="/", domain=self.request.headers.get("HOST"), overwrite=True)
        return self.response


class BeaconHandler(RequestHandler):

    def get(self, dataset_id, table_id="measurement", project_id=PROJECT_ID):
        logging.debug(self.request.params) # Debug
        row = modify_row_beacon(self.request)

        # GTM SPAM (option)
        if row[u"referrer"] == u"gtm-msr.appspot.com":
            self.response.status = 500
            return self.response

        table_id = table_id + datetime.datetime.fromtimestamp(row[u"time"]).strftime("%Y%m%d")
        body = generate_insert_body(row)

        bq = BigQuery(project_id)
        try:
            res = bq.stream_row(dataset_id, table_id, body)
        except HttpError, e:
            content = json.loads(e.content)
            message = content[u"error"][u"message"]
            if message == "Not found: {0} {1}:{2}".format('Dataset', project_id, dataset_id):
                logging.warning(message)
                # Create Dataset
                try:
                    res2 = bq.create_dataset(dataset_id)
                    logging.info(json.dumps(res2))
                except Exception, e:
                    logging.error(e)
                # Create Table
                schema = modify_schema(row)
                try:
                    res2 = bq.create_table(dataset_id, table_id, schema)
                    logging.info(json.dumps(res2))
                except Exception, e:
                    logging.error(e)
                # Retry Insert
                res = {
                    u"kind": u"bigquery#tableDataInsertAllResponse",
                    u"insertErrors": [{
                        u"index": 0,
                        u"errors": [{
                            u"reason": u"invalid",
                            u"message": message,
                        }]
                    }]
                }
            elif message == "Not found: {0} {1}:{2}.{3}".format('Table', project_id, dataset_id, table_id):
                logging.warning(message)
                # Create Table
                schema = modify_schema(row)
                try:
                    res = bq.create_table(dataset_id, table_id, schema)
                    logging.info(json.dumps(res))
                except Exception, e:
                    logging.error(e)
                # Retry Insert
                res = {
                    u"kind": u"bigquery#tableDataInsertAllResponse",
                    u"insertErrors": [{
                        u"index": 0,
                        u"errors": [{
                            u"reason": u"invalid",
                            u"message": message,
                        }]
                    }]
                }
            else:
                logging.error(content)
        except Exception, e:
            logging.error(e)

        if u"insertErrors" in res:
            message = res[u"insertErrors"][0][u"errors"][0][u"message"]
            if message == u"no such field":
                logging.warning(json.dumps(res))
                # Update Table
                schema = modify_schema(row)
                try:
                    res2 = bq.update_table(dataset_id, table_id, schema)
                    logging.info(json.dumps(res2))
                except Exception, e:
                    logging.error(e)
            else:
                logging.warning(json.dumps(res))
            # Retry Insert
            taskqueue.add(url="/dmp/task/event/{0}/{1}".format(dataset_id, table_id), payload=json.dumps(body), queue_name="reinsert")
            logging.info("TaskQueue Add (insertId: {0})".format(body[u"insertId"]))
        else:
            pass

        self.response.headerlist = [
            ("Access-Control-Allow-Headers", "Content-Type, Cookie"),
            ("Access-Control-Allow-Methods", "GET"),
            ("Access-Control-Allow-Origin", "*"),
            ("Content-Type", "image/gif"),
            ("Cache-Control", "private, no-store, no-cache, max-age=0, proxy-revalidate"),
        ]
        self.response.set_cookie("bqid", value=row["bqid"], max_age=60*60*24*365*1, path='/', domain=self.request.headers.get("HOST"), overwrite=True)
        self.response.body = GIF_PIXEL
        self.response.status = 200
        return self.response


class EventHandler(RequestHandler):

    def get(self, dataset_id, table_id, project_id=PROJECT_ID):
        apikey = self.request.params.get("api_key")
        callback = self.request.params.get("callback")
        row = modify_row_td(self.request)

        # GTM SPAM (option)
        if row[u"td_host"] == u"gtm-msr.appspot.com":
            self.response.status = 500
            return self.response

        table_id = table_id + datetime.datetime.fromtimestamp(row[u"time"]).strftime("%Y%m%d")
        body = generate_insert_body(row)
        bq = BigQuery(project_id)
        try:
            res = bq.stream_row(dataset_id, table_id, body)
        except HttpError, e:
            content = json.loads(e.content)
            message = content[u"error"][u"message"]
            if message == "Not found: {0} {1}:{2}".format('Dataset', project_id, dataset_id):
                logging.warning(message)
                # Create Dataset
                try:
                    res2 = bq.create_dataset(dataset_id)
                    logging.info(json.dumps(res2))
                except Exception, e:
                    logging.error(e)
                # Create Table
                schema = modify_schema(row)
                try:
                    res2 = bq.create_table(dataset_id, table_id, schema)
                    logging.info(json.dumps(res2))
                except Exception, e:
                    logging.error(e)
                # Retry Insert
                res = {
                    u"kind": u"bigquery#tableDataInsertAllResponse",
                    u"insertErrors": [{
                        u"index": 0,
                        u"errors": [{
                            u"reason": u"invalid",
                            u"message": message,
                        }]
                    }]
                }
            elif message == "Not found: {0} {1}:{2}.{3}".format('Table', project_id, dataset_id, table_id):
                logging.warning(message)
                # Create Table
                schema = modify_schema(row)
                try:
                    res = bq.create_table(dataset_id, table_id, schema)
                    logging.info(json.dumps(res))
                except Exception, e:
                    logging.error(e)
                # Retry Insert
                res = {
                    u"kind": u"bigquery#tableDataInsertAllResponse",
                    u"insertErrors": [{
                        u"index": 0,
                        u"errors": [{
                            u"reason": u"invalid",
                            u"message": message,
                        }]
                    }]
                }
            else:
                logging.error(content)
        except Exception, e:
            logging.error(e)
            self.response.headers = DEFAULT_HEADERS
            self.response.body = "typeof {0} === 'function' && {0}({{\"created\":{1}}});".format(callback, "false")
            self.response.status = 200
            return self.response

        if u"insertErrors" in res:
            message = res[u"insertErrors"][0][u"errors"][0][u"message"]
            if message == u"no such field":
                logging.warning(json.dumps(res))
                # Update Table
                schema = modify_schema(row)
                try:
                    res2 = bq.update_table(dataset_id, table_id, schema)
                    logging.info(json.dumps(res2))
                except Exception, e:
                    logging.error(e)
            else:
                logging.warning(json.dumps(res))
            # Retry Insert
            taskqueue.add(url="/dmp/task/event/{0}/{1}".format(dataset_id, table_id), payload=json.dumps(body), queue_name="reinsert")
            logging.info("TaskQueue Add (insertId: {0})".format(body[u"insertId"]))
        else:
            pass

        self.response.headers = DEFAULT_HEADERS
        self.response.content_type = "application/javascript"
        self.response.set_cookie("bqid", value=row["bqid"], max_age=60*60*24*365*1, path='/', domain=self.request.headers.get("HOST"), overwrite=True)
        self.response.body = "typeof {0} === 'function' && {0}({{\"created\":{1}}});".format(callback, "true")
        self.response.status = 200
        return self.response


class TaskeventHandler(RequestHandler):

    def post(self, dataset_id, table_id, project_id=PROJECT_ID):
        body = json.loads(self.request.body)
        logging.info("TaskQueue Pull (insertId: {0})".format(body[u"insertId"]))
        bq = BigQuery(project_id)
        try:
            res = bq.stream_row(dataset_id, table_id, body)
        except Exception, e:
            logging.warning(e)
            self.response.status = 400 # RETRY
            return self.response

        if u"insertErrors" in res:
            logging.warning(json.dumps(res))
            self.response.status = 400 # RETRY
            return self.response
        else:
            logging.info(json.dumps(res))
            self.response.status = 200
            return self.response
