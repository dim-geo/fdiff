#!/usr/bin/env python
# -*- coding: utf-8 -*-

#    Copyright (C) 2010  Dimitrios Georgiou <dim.geo at gmail.com>
#
#    This program can be distributed under the terms of the GPLv3.
#

import os, sys, shelve, StringIO, bsdiff, pickle, zlib
from errno import *
from stat import *
import fuse
from fuse import Fuse

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')

dfiles=dict()
datastore=dict()

def objecttozip(data):
    return zlib.compress(pickle.dumps(data))

def ziptoobject(zdata):
    return pickle.loads(zlib.decompress(zdata))

def getoriginalname(name):
    dper=shelve.open(datastore['a'],flag = 'r')
    if dper.has_key(name):
        return dper[name][2]
    else:
        return getoriginalpath(name)
    dper.close()

def getoriginalpath(path):
    dper=shelve.open(datastore['a'],flag = 'r')
    keylen=0 # slashes....
    specialkey=''
    for key in dper.keys():
        if path.startswith(key):
            if len(key) - len(key.translate(None,'/')) > keylen:
                specialkey=key
                keylen=len(key) - len(key.translate(None,'/'))
    #print specialkey, keylen
    if(specialkey!=''):
        realpath=path.replace(specialkey,dper[specialkey][2])
    else:
        realpath=path
    #print path, realpath
    dper.close()
    return realpath

def getoriginalfile(path):
    realpath=getoriginalpath(path)
    originalfile=StringIO.StringIO()
    with open(realpath,'rb') as fl:
        originalfile.write(fl.read())
    originalfile.seek(0)
    return originalfile

def getmodifiedfile(path):
    modifiedfile=StringIO.StringIO()
    dper=shelve.open(datastore['a'],flag = 'r')
    if dper.has_key(path):
        zdiff,new_len,oldpath=dper[path]
        original_file=getoriginalfile(oldpath)
        original_data=original_file.read()
        original_file.close()
        if zdiff=='':
            modifiedfile.write(original_data)
        else:
            newdiff=ziptoobject(zdiff)
            new_data=bsdiff.Patch(original_data,new_len,newdiff[0],newdiff[1],newdiff[2])
            modifiedfile.write(new_data)
    else:
        original_file=getoriginalfile(path)
        original_data=original_file.read()
        original_file.close()
        modifiedfile.write(original_data)
    modifiedfile.seek(0)
    dper.close()
    return modifiedfile

def setmodifiedfile(path,newfile):
    modfile=getmodifiedfile(path)
    moddata=modfile.read()
    modfile.close()
    pos=newfile.tell()
    newfile.seek(0)
    newdata=newfile.read()
    newfile.seek(pos)
    if moddata != newdata:
        savedoldpath=path
        olddata=''
        dper=shelve.open(datastore['a'],flag = 'r')
        if dper.has_key(path):
            zdiff,new_len,oldpath=dper[path]
            #print oldpath
            oldfile=getoriginalfile(oldpath)
            olddata=oldfile.read()
            oldfile.close()
            savedoldpath=oldpath
        else:
            savedoldpath=getoriginalpath(path)
            oldfile=getoriginalfile(path)
            olddata=oldfile.read()
            oldfile.close()
        dper.close()
        diff=bsdiff.Diff(olddata,newdata)
        full_diff=objecttozip(diff),len(newdata),savedoldpath
        dpersistence=shelve.open(datastore['a'],flag = 'w', writeback=True)
        dpersistence[path] = full_diff
        dpersistence.close()

class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class FDiff(Fuse):

    def __init__(self, *args, **kw):

        Fuse.__init__(self, *args, **kw)

        self.root = '/home'
        self.datastorage='/home/datapersistence'

    def getattr(self, path):
        st=MyStat()
        name="." + path
        realname=getoriginalname(name)
        mdata = os.lstat(realname)
        st.st_mode = mdata.st_mode
        st.st_ino = mdata.st_ino
        st.st_dev = mdata.st_dev
        st.st_nlink = mdata.st_nlink
        st.st_uid = mdata.st_uid
        st.st_gid = mdata.st_gid
        st.st_size = mdata.st_size
        st.st_atime = mdata.st_atime
        st.st_mtime = mdata.st_mtime
        st.st_ctime = mdata.st_ctime
        #print name
        dper=shelve.open(self.datastorage,flag = 'r')
        #for key in dper.keys():
        #    print key, dper[key][2]
        #print '\n'
        if dper.has_key(name):
            #print dper[name][2]
            st.st_size = dper[name][1]
        dper.close()
        if dfiles.has_key(name) and not dfiles[name].closed:
            pos=dfiles[name].tell()
            dfiles[name].seek(0)
            st.st_size=len(dfiles[name].read())
            dfiles[name].seek(pos)
        #print st.st_size
        return st

    def readdir(self, path, offset):
        #print "." + path
        lspath="." + path
        reallspath=getoriginalpath(lspath)
        oldlist=os.listdir(reallspath)
        #print oldlist
        dper=shelve.open(self.datastorage,flag = 'r')
        for k,v in dper.iteritems():
            if path!='/':
                newname=k.replace(lspath+'/','')
                oldname=v[2].replace(reallspath+'/','')
            else:
                newname=k.replace(lspath,'')
                oldname=v[2].replace(reallspath,'')                
            #print newname,oldname
            if oldname.find('/') == -1:
                oldlist.remove(oldname)
            if newname.find('/') == -1:
                oldlist.append(newname)
        dper.close()
        #print oldlist
        print '\n'
        for e in oldlist:
            yield fuse.Direntry(e)

    def rename(self, path, path1):
        oldname="." + path
        newname="." + path1
        #print oldname, newname
        if oldname == newname:
            return
        #if  not os.path.exists(oldname):
        #    return
        #if os.path.exists(newname):
        #    return
        dper=shelve.open(self.datastorage,flag = 'w', writeback=True)
        for key in dper.keys():
            if key.startswith(oldname):
                newkey=key.replace(oldname,newname)
                if len(newkey)==len(newname) or newkey[len(newname)]=='/':
                    dper[newkey]=dper[key]
                    del dper[key]
                    print key
        dper.close()
        realoldname=getoriginalpath(oldname)
        dper=shelve.open(self.datastorage,flag = 'w', writeback=True)
        if not dper.has_key(newname):
            mdata=os.lstat(realoldname)
            dper[newname]='',mdata.st_size,realoldname
        #if dper.has_key(oldname):
        #    dper[newname]=dper[oldname]
        #    del dper[oldname]
        #else:
        #    mdata=os.lstat(oldname)
        #    dper[newname]='',mdata.st_size,oldname
        dper.close()

    def truncate(self, path, len):
        if dfiles.has_key("." + path) and not dfiles["." + path].closed:
            dfiles["." + path].truncate(len)
            setmodifiedfile("." + path,dfiles["." + path])
        else:
            myfile=getmodifiedfile("." + path)
            myfile.truncate(len)
            setmodifiedfile("." + path,myfile)
            myfile.close()
    
    def unlink(self, path):
        dper=shelve.open(self.datastorage,flag = 'w', writeback=True)
        if dper.has_key("." + path):
            del dper["." + path]
        #else:
        #    os.unlink("." + path)
        dper.close()

    def statfs(self):
        return os.statvfs(".")

    def fsinit(self):
        #print self.root
        #print self.datastorage
        dper=shelve.open(self.datastorage,flag = 'c', writeback=True)
        dper.close()
        datastore['a']=self.datastorage
        #print self.fuse_args.mountpoint
        os.chdir(self.root)

    class FDiffFile(object):

        def __init__(self, path, flags, *mode):
            self.nam="."+path
            dfiles["."+path]=self.file=getmodifiedfile("."+path)

        def read(self, length, offset):
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            self.file.seek(offset)
            self.file.write(buf)            
            return len(buf)

        def release(self, flags):
            self.file.close()

        def _fflush(self):
            setmodifiedfile(self.nam,self.file)

        def fsync(self, isfsyncfile):
            self._fflush()

        def flush(self):
            self._fflush()

        def fgetattr(self):
            st=MyStat()
            realname=getoriginalname(self.nam)
            mdata = os.stat(realname)
            st.st_mode = mdata.st_mode
            st.st_ino = mdata.st_ino
            st.st_dev = mdata.st_dev
            st.st_nlink = mdata.st_nlink
            st.st_uid = mdata.st_uid
            st.st_gid = mdata.st_gid
            st.st_size = mdata.st_size
            st.st_atime = mdata.st_atime
            st.st_mtime = mdata.st_mtime
            st.st_ctime = mdata.st_ctime
            dper=shelve.open(datastore['a'],flag = 'r')
            if dper.has_key(self.nam):
                st.st_size = dper[self.nam][1]
            dper.close()
            pos=self.file.tell()
            self.file.seek(0)
            st.st_size=len(self.file.read())
            self.file.seek(pos)
            return st

        def ftruncate(self, len):
            self.file.truncate(len)

    def main(self, *a, **kw):

        self.file_class = self.FDiffFile

        return Fuse.main(self, *a, **kw)


def main():

    usage = """
mirror the filesystem tree from some point on. Store differences in a file.

""" + Fuse.fusage

    server = FDiff(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
     help="mirror filesystem from under PATH [default: %default]")
    server.parser.add_option(mountopt="datastorage",metavar="PATH",default='/home/datapersistence',
     help="file which stores data [default: %default]")
    server.parse(values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print >> sys.stderr, "can't enter root of underlying filesystem"
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main()
