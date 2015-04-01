#!/usr/bin/python
#

import MySQLdb         # For our MySQL connections
import MySQLdb.cursors # So we can return results as dicts instead of tuples
#import pyodbc          # For other connections that may be required in the future
import time
import sys
import db_settings
from pycommon.util.util import *

class db:
    """ Manages db connections for local tools """
    conn = {}

    def __init__(self, db="reflex_relations_2014", thread="local"):
        """ Initialize the object, default connection will be to our local db """
        self.db = {}
        if db:
            key = db + "-" + thread
            if db not in db_settings.db:
                out("[ERROR] Unknown db key: " + db)
                out("  Add to db_settings.py. Exiting.")
                raise Exception('MissingDB', 'MissingDB')
            # Default source is MySQL
            if "source" not in db_settings.db[db]:
                db_settings.db[db]["source"] = "MySQL"
            # Default port is 3306
            if "port" not in db_settings.db[db]:
                db_settings.db[db]["port"] = 3306

            if db_settings.db[db]["source"] == "MySQL":
                out("Creating new MySQL DB connection for key: " + key)
                self.db['conn'] = MySQLdb.connect(
                    host = db_settings.db[db]['host'], port = int(db_settings.db[db]["port"]), 
                    db = db_settings.db[db]['db'], 
                    user = db_settings.db[db]['user'], passwd = db_settings.db[db]['pass'], 
                    use_unicode=True, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
                self.db['conn'].autocommit(True)
                self.db['cursor'] = self.db['conn'].cursor()
                self.db['db'] = db
                self.db['thread'] = thread
                self.db['key'] = key
            elif db_settings.db[db]["source"] == "SomethingElse":
                out("Creating new other DB connection for key: " + key)
            else:
                out("[ERROR] Unknown db source: " + db_settings.db[db]["source"])
                out("  Add to mdg_db.py -> getCursorForDB.  Exiting.")
                raise Exception('MissingSource', 'MissingSource')
        

    def cursor(self):
        """ Returns a new cursor for the db connection """
        cursor = self.db["conn"].cursor()
        self.db["cursor"] = cursor
        return cursor

    def close(self):
        """ Closes the connection """
        self.db["conn"].close()
        return True

    def escape_string(self, string):
        """ Escapes a query string """
        s = string
        try:
            s = string.encode("utf8")
        except:
            pass
        return self.db["conn"].escape_string(s)

    def renewConnection(self):
        """ Renews a potentially closed DB connection """
        try:
            self.db["conn"].close()
        except:
            pass
        time.sleep(5)
        return self.__init__(self.db["db"], self.db["thread"])

    def execute(self, query, values=(), count=4):
        """ Executes a query with a given cursor. If the DB connection is lost, will 
            retry <count> times """
        if count > 0:
            try:
                self.db["cursor"].execute(query, values)
                #if count < 4:
                #    key = self.getKeyFromCursor(cursor)
                #    out("[DB] " + self.conn[key]['thread'] + " Recovered from DB error!\n\n")
            except:
                err = str(sys.exc_info()[1])
                if err.find("MySQL server has gone away") != -1:
                    out(self.db["key"] + " - Lost connection to MySQL server, attempting to reconnect.")
                    time.sleep(5)
                    cursor = self.renewConnection()
                    self.execute(query, values=values, count=count-1)
                else:
                    out("[ERROR] " + self.db["thread"] + " - DB query failed: " + err)
                    #out("    Query: " + query)
                    #out("    Values: " + ','.join(map(str, values)))
        else:
            out("[ERROR] Exceeded failure count for this query. Exiting.")
            raise Exception("MaxFailuresReached", "Multiple failures attempting to execute query.")

        return self.db["cursor"]


