import os
import socket
import ConfigParser
import json
import hashlib
import shutil
from operator import itemgetter
from functools import partial

SYNC_HOME = '/home/toni/Dropbox/sync'
CONFIG_FILE = os.path.join(SYNC_HOME,'config')

settings_defaults = {'maxfiles':10,
                     'maxsize':100*(2 ** 20)} #100 mb

def cmptuples((a1,b1),(a2,b2)):
    c = cmp(b1,b2)
    if c!=0:
        return c
    return cmp(a1,a2)

def loadconfig():

    hostname = socket.gethostname()

    config = ConfigParser.ConfigParser(settings_defaults)
    config.read(CONFIG_FILE)

    settings = {}
    settings['synchome'] = SYNC_HOME
    settings['maxfiles'] = int(config.get('settings','maxfiles',raw=True))
    settings['maxsize'] = int(config.get('settings','maxsize',raw=True))

    instances_names = [s for s in config.sections() if s != 'settings']

    def makeinstanceconfig(section):

        instance_config = {}
        instance_config['name'] = section
        instance_config['path'] = config.get(section,'path')
        instance_config['master'] = config.get(section,'master')
        instance_config['slaves'] = config.get(section,'slaves').split(',')

        return (section,instance_config)

    instances = dict(map(makeinstanceconfig,instances_names))

    config = {'settings':settings,
              'instances':instances,
              'hostname':hostname}

    return config

def mkdir(path):
    try:
        os.makedirs(path)
        print 'created dir "%s"' % (path,)
    except OSError:
        pass
    
def copy(srcfile,destfile):
    destdir = os.path.dirname(destfile)
    mkdir(destdir)
    shutil.copy(srcfile,destfile)

def crawl(rootdir):
    for root, dirnames, filenames in os.walk(rootdir):
        dirpath = root[len(rootdir)+1:]
        for base in filenames:
            yield unicode(os.path.join(dirpath,base),'UTF8')
            
def hash(string):
    return hashlib.md5(repr(string).lstrip('u')).hexdigest()
    
class Resolve(object):

    @staticmethod
    def fromconfig(settingsconf,instanceconf,hostname):
        resolveclass = None
        if instanceconf['master'] == hostname:
            resolveclass = MasterResolve
        elif hostname in instanceconf['slaves']:
            resolveclass = SlaveResolve

        if resolveclass:
            return resolveclass(settingsconf,instanceconf,hostname)

    def __init__(self,settingsconf,instanceconf,hostname):
        self.settingsconf = settingsconf
        self.instanceconf = instanceconf
        self.hostname = hostname
        self.rootdir = instanceconf['path']

        self.instancename = self.instanceconf['name']
        self.instancedir = os.path.join(self.settingsconf['synchome'],'instances',self.instancename)
        self.datadir = os.path.join(self.instancedir,'data')
        self.metadir = os.path.join(self.instancedir,'meta')
        self.metadirlock = os.path.join(self.instancedir,'meta','lock.txt')
        self.metafile = os.path.join(self.metadir,'%s.txt' % self.hostname)
        
        self.thisfilelist = None
        self.hashedsyncfilelist = None
        
        self.masterfilelist = None
        
        self.slavefilelists = []
        self.slavefilecount = []
        
        self.synctotalsum = None
        self.synctotalhash = None
        
    def jointhis(self,basepath):
        return os.path.join(self.rootdir,basepath)
    
    def joinsync(self,basepath):
        return os.path.join(self.datadir,basepath)

    def isready(self):
        if not( os.path.exists(self.datadir) and os.path.exists(self.metadir)):
            return False
        
        return self.checklock()
    
    def loadthisfilelist(self):
        self.thisfilelist = list(crawl(self.rootdir))
        print 'found %d files in this dir' % (len(self.thisfilelist),)
        
    def loadsyncfilelist(self):
        self.hashedsyncfilelist = [path for path in os.listdir(self.datadir)]
        print 'found %d files in sync dir' % (len(self.hashedsyncfilelist),)
        
    def dumpmeta(self,force=False):
        if force:
            self.loadthisfilelist()
        with open(self.metafile,'w') as f:
            json.dump(self.thisfilelist,f,indent=0)
        print 'dumped meta to "%s"' % (self.metafile)
            
    def calculatelock(self):
        basenames = sorted(os.listdir(self.datadir))
        paths = map(partial(os.path.join,self.datadir),basenames)
        totalsum = sum(map(os.path.getsize,paths))
        totalhash = hash(''.join(basenames))
        return (totalsum,totalhash)
    
    def dumplock(self):
        (totalsum,totalhash) = self.calculatelock()
        with open(self.metadirlock,'w') as f:
            json.dump((totalsum,totalhash),f)
            
    def loadlock(self):
        with open(self.metadirlock,'r') as f:
            (totalsum,totalhash) = json.load(f)
        return (totalsum,totalhash)
            
    def checklock(self):
        return self.calculatelock() == self.loadlock()
            
    def loadmeta(self,instancename):
        instancemetafile = os.path.join(self.metadir,instancename+'.txt')
        if os.path.exists(instancemetafile):
            with open(instancemetafile,'r') as f:
                instancefilelist = json.load(f)
            print 'loaded meta file for instance "%s"' % (instancename,)
        else:
            instancefilelist = []
            print 'warning - no meta file for instance "%s"' % (instancename,)
        return instancefilelist
           
class MasterResolve(Resolve):
    
    def makedirs(self):
        map(mkdir,(self.datadir,self.metadir))

    def resolve(self):
        self.makedirs()
        self.loadthisfilelist()
        self.dumpmeta()
        self.loadslavemeta()
        self.loadsyncfilelist()
        
        self.this2sync()
        
    def loadslavemeta(self):
        self.slavefilelists = {}
        self.slavefilecount = {}
        
        for slaveinstance in self.instanceconf['slaves']:
            slavefilelist = self.loadmeta(slaveinstance)
            self.slavefilelists[slaveinstance] = slavefilelist
            
            for path in slavefilelist:
                self.slavefilecount[path] = self.slavefilecount.setdefault(path,0)+1
                
        map(lambda p: self.slavefilecount.setdefault(p,0),self.thisfilelist)
                
    def files2sync(self):
        slavefilecountitems = self.slavefilecount.items()
        sortedslavefilecountitems = sorted(filter(lambda (a,b) : b < len(self.instanceconf['slaves']),slavefilecountitems),cmp=cmptuples)
        return map(itemgetter(0),sortedslavefilecountitems)
    
    def this2sync(self):
        thisfilelist = self.files2sync()
        
        limitedthisfilelist = thisfilelist[:self.settingsconf['maxfiles']]
        limitedhashedthisfilelist = map(hash,limitedthisfilelist)
        
        #delete not needed files
        hashedsycnfilelist_todelete = set(self.hashedsyncfilelist) - set(limitedhashedthisfilelist)
        for hashedfilename in hashedsycnfilelist_todelete:
            srcfile = self.joinsync(hashedfilename)
            os.remove(srcfile)
            print '%r deleted from sync dir' % (srcfile,)
            
        for (filename,hashedfilename) in zip(limitedthisfilelist,limitedhashedthisfilelist):
            srcfile = self.jointhis(filename)
            destfile = self.joinsync(hashedfilename)
            
            if not os.path.exists(destfile):
                print '%r -> %r' % (srcfile,destfile)
                shutil.copy(srcfile,destfile)
            else:
                print '%r - ignored (already in sync dir)' % (filename,)
                
        self.dumplock()
            
class SlaveResolve(Resolve):
    
    def loadmastermeta(self):
        self.masterfilelist = self.loadmeta(self.instanceconf['master'])
        
    def sync2this(self):
        
        hashfilelistdict = dict((hash(path),path) for path in self.masterfilelist)
        
        for hashedsyncfile in self.hashedsyncfilelist:
            filename = hashfilelistdict[hashedsyncfile]
            srcfile = self.joinsync(hashedsyncfile)
            destfile = self.jointhis(filename)
            
            if not os.path.exists(destfile):
                print '%r -> %r' % (srcfile,destfile)
                copy(srcfile,destfile)
            else:
                print '%r - ignored (already in this dir)' % (filename,)

    def printdiff(self):
        masterfilesset = set(self.masterfilelist)
        thisfileset = set(self.thisfilelist)
    
        onlymasterfileset = masterfilesset - thisfileset
        onlyslavefileset = thisfileset - masterfilesset
        
        print 'master: %d files (%d not in slave), slave: %d files (%d not in master)' % \
              (len(masterfilesset),len(onlymasterfileset),len(thisfileset),len(onlyslavefileset))
                
    def resolve(self):
        if self.isready():
            print 'ready!'
            self.loadthisfilelist()
            self.loadmastermeta()
            self.printdiff()
            self.loadsyncfilelist()
            self.sync2this()
            self.dumpmeta(force=True)
        else:
            print 'not ready!'

if __name__ == '__main__':
    config = loadconfig()
    print config

    settings = config['settings']
    hostname = config['hostname']
    #hostname = 'slave1'

    for instance in config['instances'].values():
        r = Resolve.fromconfig(settings,instance,hostname)
        if r:
            r.resolve()


