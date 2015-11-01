"""A database logger that uses dataset (a wrapper around SQLAlchemy) to log to a variety of
databases"""
from cowrie.core import dblog
from twisted.internet import defer
import dataset
import uuid


class DBLogger(dblog.DBLogger):
    def start(self, cfg):
        dburl = cfg.get('database_anydb', 'url')
        self.db = dataset.connect(dburl)

    def createSession(self, peerIP, peerPort, hostIP, hostPort):
        sid = self.db['session'].insert(dict(starttime=self.nowUnix(),
                                             sensor=self.getSensor(),
                                             peerIP=peerIP,
                                             peerPort=peerPort,
                                             hostIP=hostIP))
        return sid

    def handleConnectionLost(self, session, args):
        ttylog = self.ttylog(session)
        if ttylog:
            self.db['ttylog'].insert(dict(session=session, ttylog=self.ttylog(session)))
        self.db['session'].update(dict(session=session, endtime=self.nowUnix()), ['session'])

    def handleLoginFailed(self, session, args):
        self.db['auth'].insert(dict(session=session,
                                    success=0,
                                    username=args['username'],
                                    password=args['password'],
                                    timestamp=self.nowUnix()))

    def handleLoginSucceeded(self, session, args):
        self.db['auth'].insert(dict(session=session,
                                    success=1,
                                    username=args['username'],
                                    password=args['password'],
                                    timestamp=self.nowUnix()))

    def handleCommand(self, session, args):
        self.db['input'].insert(dict(session=session,
                                     timestamp=self.nowUnix(),
                                     success=1,
                                     input=args['input']))

    def handleUnknownCommand(self, session, args):
        self.db['input'].insert(dict(session=session,
                                     timestamp=self.nowUnix(),
                                     success=0,
                                     input=args['input']))

    def handleInput(self, session, args):
        self.db['input'].insert(dict(session=session,
                                     timestamp=self.nowUnix(),
                                     realm=args['realm'],
                                     input=args['input']))

    def handleTerminalSize(self, session, args):
        self.db['session'].update(dict(session=session,
                                       termheight=args['height'],
                                       termwidth=args['width']), ['session'])

    def handleClientVersion(self, session, args):
        clientid = None
        client = self.db['client'].find_one(version=args['version'])
        if client is None:
            clientid = self.db['client'].insert(dict(version=args['version']))
        else:
            clientid = client['id']
        self.db['session'].update(dict(session=session, client=clientid), ['session'])

    def handleFileDownload(self, session, args):
        self.db['downloads'].insert(dict(session=session,
                                         timestamp=self.nowUnix(),
                                         url=args['url'],
                                         outfile=args['outfile'],
                                         shasum=args['shasum']))
