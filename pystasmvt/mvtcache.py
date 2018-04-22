import os
from mbutil import util 
import sqlite3
import gzip
import json
import logging
import threading

logger = logging.getLogger(__name__)

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
        self._lock = threading.Lock()
    
    def set_tile(self,group,x,y,z,binary):
        with self._lock:
            set_cache_to_file(self._dir,group,z,x,y,binary)

    def get_tile(self,group,x,y,z):
        with self._lock:
            return get_cache_from_file(self._dir,group,z,x,y)

def mbtiles_connect(mbtiles_file, silent):
    """
    https://github.com/mapbox/mbutil/blob/master/mbutil/util.py
    """
    try:
        con = sqlite3.connect(mbtiles_file)
        return con
    except Exception as e:
        if not silent:
            logger.error("Could not connect to database")
            logger.exception(e)
        raise

def mbtiles_setup(cur):
    """
    https://github.com/mapbox/mbutil/blob/master/mbutil/util.py
    """
    cur.execute("""
        create table IF NOT EXISTS tiles (
            zoom_level integer,
            tile_column integer,
            tile_row integer,
            tile_data blob);
            """)
    cur.execute("""create table IF NOT EXISTS metadata
        (name text, value text);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS grids (zoom_level integer, tile_column integer,
    tile_row integer, grid blob);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS grid_data (zoom_level integer, tile_column
    integer, tile_row integer, key_name text, key_json text);""")
    cur.execute("""create unique index IF NOT EXISTS  name on metadata (name);""")
    cur.execute("""create unique index IF NOT EXISTS tile_index on tiles
        (zoom_level, tile_column, tile_row);""")

def make_mbtiles_temp_file(mbtiles_file):
    con = mbtiles_connect(mbtiles_file, True)
    cur = con.cursor()
    mbtiles_setup(cur)

def _insert_mbtiles_tile(cur,x,y,z,binary):
    """
    #pbfはgzipで圧縮するという仕様
    https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
    """
    cur.execute(
        ('replace into tiles (zoom_level,tile_column, tile_row, tile_data)' 
        'values(?, ?, ?, ?);'),
        (int(z), int(x), int(y), sqlite3.Binary(gzip.compress(binary))))

def _get_mbtiles_tile(con,x,y,z):
    """
    #pbfはgzipで圧縮するという仕様
    https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
    """
    tiles = con.execute(
            ('SELECT zoom_level, tile_column, tile_row, tile_data '
            'FROM tiles '
            'WHERE zoom_level == {0} AND tile_column == {1} AND tile_row == {2};'
            ).format(int(z),int(x),int(y))
        )
    t = tiles.fetchone()
    if t :
        return gzip.decompress(t[3])
    else:
        return None

def _insert_mbtiles_metadata(cur,metadata):
    for name,value in metadata.items():
        if name == 'json':
            json_str = json.dumps(value)
            cur.execute('replace into metadata (name, value) values (?, ?)',(name, json_str))
        else:
            cur.execute('replace into metadata (name, value) values (?, ?)',(name, str(value)))

class Mbtiles(object):

    def __init__(self,mbtiles_file):
        #self._con = mbtiles_connect(mbtiles_file, True)
        self._mbtiles_file = mbtiles_file
        con = mbtiles_connect(self._mbtiles_file, True)
        cur = con.cursor()
        #util.optimize_connection(cur)
        mbtiles_setup(cur)
        cur.close()

    def set_metadata(self,metadata):
        con = mbtiles_connect(self._mbtiles_file, True)
        cur = con.cursor()
        _insert_mbtiles_metadata(cur,metadata)
        con.commit()
        cur.close()

    def set_tile(self,x,y,z,binary):
        con = mbtiles_connect(self._mbtiles_file, True)
        cur = con.cursor()
        _insert_mbtiles_tile(cur,x,y,z,binary)
        con.commit()
        cur.close()

    def get_tile(self,x,y,z):
        con = mbtiles_connect(self._mbtiles_file, True)
        return _get_mbtiles_tile(con,x,y,z)

class MbtilesCache(object):
    def __init__(self,dir):
        self._dir=dir
        self._mbtiles_dic={}
    
    def set_tile(self,group,x,y,z,binary):
        try:
            if group not in self._mbtiles_dic.keys():
                self._mbtiles_dic[group] = Mbtiles(os.path.join(self._dir,group+'.mbt'))
            self._mbtiles_dic[group].set_tile(x,y,z,binary)
        except Exception as e:
            logger.error("MbtilesCache set_tile")
            logger.exception(e)
            raise

        
    def get_tile(self,group,x,y,z):
        try:
            if group not in self._mbtiles_dic.keys():
                return None
            return self._mbtiles_dic[group].get_tile(x,y,z)
        except Exception as e:
            logger.error("MbtilesCache get_tile")
            logger.exception(e)
            raise
        