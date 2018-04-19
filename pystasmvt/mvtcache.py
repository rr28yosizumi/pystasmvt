import os
from mbutil import util 
import sqlite3
import gzip
import json

def get_cache_from_file(basepath,group,zoom,x,y):
    path = os.path.join(basepath,group,zoom,x,'{0}.pbf'.format(y))
    if os.path.exists(path):
        binary=None
        with open(path,'rb') as file:
            binary = file.read()
        return binary
    else:
        return None

def set_cache_to_file(basepath,group,zoom,x,y,binary):
    path = os.path.join(basepath,group,zoom,x,'{0}.pbf'.format(y))
    if not os.path.exists(path):
        dirpath = os.path.join(basepath,group,zoom,x)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        with open(path,'wb') as file:
            file.write(binary)

class FileCache(object):
    def __init__(self,dir):
        self._dir=dir
    
    def set_tile(self,group,x,y,z,binary):
        set_cache_to_file(self._dir,group,z,x,y,binary)

    def get_tile(self,group,x,y,z):
        return get_cache_from_file(self._dir,group,z,x,y)


def make_mbtiles_temp_file(mbtiles_file):
    con = util.mbtiles_connect(mbtiles_file, True)
    cur = con.cursor()
    util.optimize_connection(cur)
    util.mbtiles_setup(cur)

def _insert_mbtiles_tile(cur,x,y,z,binary):
    """
    #pbfはgzipで圧縮するという仕様
    https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
    """
    cur.execute(
        ('insert into tiles (zoom_level,tile_column, tile_row, tile_data)' 
        'values(?, ?, ?, ?);'),
        (z, x, y, sqlite3.Binary(gzip.compress(binary))))

def _get_mbtiles_tile(con,x,y,z):
    """
    #pbfはgzipで圧縮するという仕様
    https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
    """
    tiles = con.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    return gzip.decompress(t[3])

def _insert_mbtiles_metadata(cur,metadata):
    for name,value in metadata.items():
        if name == 'json':
            json_str = json.dumps(value)
            cur.execute('replace into metadata (name, value) values (?, ?)',(name, json_str))
        else:
            cur.execute('replace into metadata (name, value) values (?, ?)',(name, str(value)))

class MbtilesCache(object):

    def __init__(self,mbtiles_file):
        self._con = util.mbtiles_connect(mbtiles_file, True)
        self._cur = self._con.cursor()
        util.optimize_connection(self._cur)
        util.mbtiles_setup(self._cur)

    def set_metadata(self,metadata):
        _insert_mbtiles_metadata(self._cur,metadata)

    def set_tile(self,x,y,z,binary):
        _insert_mbtiles_tile(self._cur,x,y,z,binary)

    def get_tile(self,x,y,z):
        return _get_mbtiles_tile(self._con,x,y,z)
