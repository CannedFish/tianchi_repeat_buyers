# -*- coding: utf8 -*-

from neo4j.v1 import GraphDatabase

import json

class MyJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(MyJSONEncoder, self).__init__(*args, **kwargs)

    def __call__(self, o):
        return self.encode(o)

    def encode(self, o):
        if isinstance(o, dict):
            output = ["%s:%s" % (key, self.encode(value)) for key, value in o.iteritems()]
            return "{" + ",".join(output) + "}"
        elif isinstance(o, (list, tuple)):
            output = [self.encode(v) for v in o]
            return "[%s]" % ",".join(output)
        else:
            return json.dumps(o)

json_encoder = MyJSONEncoder()

class RepeatBuyersDB(object):

    def __init__(self, uri, usr, pwd):
        self._driver = GraphDatabase.driver(uri, auth=(usr, pwd))

    def __del__(self):
        self._driver.close()

    def close(self):
        self._driver.close()

    def create_node(self, label, prop):
        with self._driver.session() as session:
            ret = session.run("CREATE (n:%s %s) RETURN n" % (label, json_encoder(prop)))
            for r in ret.records():
                print 'Node %s is created.' % r

    def create_nodes(self, nodes):
        """
        :param nodes: [
            {
                'label': label of this node,
                'prop': property object of this node
            },
            ...
        ]
        """
        stmts = []
        for node in nodes:
            stmts.append("(:%s %s)" % (node['label'], json_encoder(node['prop'])))

        with self._driver.session() as session:
            ret = session.run("CREATE %s" % ','.join(stmts))

        print "%d nodes created." % len(stmts)

    def create_edge(self, src, dst, label, prop=None):
        """
        :param src: set, ($key, %value)
        :param dst: set, ($key, %value)
        :param label: string
        :param prop: object
        """
        with self._driver.session() as session:
            stmt = "MATCH (a), (b) " \
                + "WHERE a.%s='%s' AND b.%s='%s' " %(src[0], src[1], dst[0], dst[1]) \
                + "CREATE (a)-[r:%s%s]->(b)" % (label, ' '+json_encoder(prop) if prop is not None else '') \
                + "RETURN r"
            print stmt
            ret = session.run(stmt)
            for r in ret.records():
                print 'Relation %s is created.' % r

db = RepeatBuyersDB("bolt://100.100.100.34:7687", "neo4j", "8882679")

