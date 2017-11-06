import pymysql

class DBHelper(object):
    def __init__(self):
        self.db = pymysql.connect(host="localhost",
                             user="adwords",
                             passwd="z3gOM4v543Ga",
                             db="ad-data",
                             use_unicode=True,
                             charset="utf8")
        self.cur = self.db.cursor()
    def execute(self, query, params, commit = True):
        self.cur.execute(query, params)
        if commit:
            self.db.commit()
        return True

    def query(self, query):
        self.db.query(query)

    def insert(self, table, data):
        q = 'INSERT INTO %s (%s) VALUES (%s)' % (table, ','.join(data.keys()),','.join(['%s']*len(data)))
        val = [str(i) for i in data.values()]
        x = self.execute(q,val)
        return self.db.insert_id()


