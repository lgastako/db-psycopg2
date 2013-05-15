import db

import psycopg2
import psycopg2.extras
import psycopg2.extensions


class Psycopg2Driver(db.drivers.Driver):
    PARAM_STYLE = "pyformat"
    URL_SCHEME = "postgresql"

    def setup_cursor(self, cursor):
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cursor)

    def ignore_exception(self, ex):
        return "no results to fetch" in str(ex)

    def connect(self):
        return self._connect(*self.conn_args, **self.conn_kwargs)

    @staticmethod
    def _connect(*args, **kwargs):
        search_path = kwargs.pop("search_path", None)
        kwargs.setdefault("connection_factory",
                          psycopg2.extras.NamedTupleConnection)
        conn = psycopg2.connect(*args, **kwargs)
        if search_path:
            conn.cursor().execute("SET search_path = %s" % search_path)
        return conn

    @classmethod
    def from_url(cls, url):
        parsed = urlparse.urlparse(url)
        if parsed.scheme != "postgresql":
            return
        return cls(

db.drivers.autoregister_class(Psycopg2Driver)
