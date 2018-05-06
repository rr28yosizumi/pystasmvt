import tornado.ioloop
import tornado.web
from tornado import gen
import io
import os
import json
import concurrent.futures

import logging

LOGGER = logging.getLogger(__name__)

from pystasmvt import mvtcreator
from pystasmvt import mvtcache

def _get_layerconfig_from_json(file):
    """ JSONファイルの読み込み
    """
    config = None
    with open(file,'r') as stream:
        config = json.load(stream)
    return config

class GetMvtTile(tornado.web.RequestHandler):

    def initialize(self,mvtcretorserver):
        self._mvtcreatorserver = mvtcretorserver

    @tornado.web.asynchronous
    @gen.engine
    def get(self,group, zoom,x,y):

        #https://stackoverflow.com/questions/11679040/using-gen-task-with-tornado-for-a-simple-function

        #https://gist.github.com/lbolla/3826189

        self.set_header("Content-Type", "application/x-protobuf")
        self.set_header("Content-Disposition", "attachment")
        self.set_header("Access-Control-Allow-Origin", "*")
        LOGGER.debug('{0}/{1}/{2}/{3}'.format(group,zoom,x,y))

        #イベントループを作るだけで非同期処理を自動的にしてくれるわけではない。
        response = yield gen.Task(self._mvtcreatorserver.fatch_tile_data,group,zoom,x,y)
        
        if response != None:
            self.write(response)
        self.finish()

class MvtCreateServer(object):

    def __init__(self,layerconfig_path,use_cache=True,cache_path ='./'):
        self._layerconfig_path = layerconfig_path
        self._executeor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        self._mvtcreator = None
        self._cache_path = cache_path
        self._cache = None
        self._use_cache = use_cache
        self._init_db_session()

    def _init_db_session(self):
        """ サーバ起動の事前準備処理

        設定ファイルの読み込み
        PREPAREの実行
        EXECUTE文のリスト作成

        """
        layers = _get_layerconfig_from_json(self._layerconfig_path)

        config = {
            'connection':{
                'user':os.getenv('POSTGRES_USER','map'),
                'password':os.getenv('POSTGRES_PASSWORD','map'),
                'host':os.getenv('POSTGRES_HOST','localhost'),
                'port':os.getenv('POSTGRES_PORT','5432'),
                'dbname':os.getenv('POSTGRES_DB','gis_test2')
            },
            'groups':layers['groups']
        }
        self._mvtcreator = mvtcreator.MvtCreator()
        self._mvtcreator.init_db_session(config)

        if self._use_cache:
            self._cache = mvtcache.MbtilesCache(self._cache_path)
            #self._cache = mvtcache.FileCache(self._cache_path)

        return True
    
    def get_GetMvtTile_Application(self):
        return (r"/tiles/([a-z_]+)/([0-9]+)/([0-9]+)/([0-9]+).pbf", GetMvtTile,{'mvtcretorserver':self})

    def fatch_tile_data(self,group,zoom,x,y,callback):
        def fatch_func():
            response = self.get_mvt(group,zoom,x,y)
            callback(response)
            
        self._executeor.submit(fatch_func)

    def get_mvt(self,group,zoom,x,y):
        response = None
        if self._use_cache:
            response = self._used_cache(group,zoom,x,y)
        else:
            response = self._unused_cache(group,zoom,x,y)
        return response

    def _unused_cache(self,group,zoom,x,y):
        response = self._mvtcreator.get_mvt(group,zoom,x,y)
        LOGGER.debug('created pbf')
        return response
    
    def _used_cache(self,group,zoom,x,y):
        response = self._cache.get_tile(group,x,y,zoom)
        if not response:
            response = self._mvtcreator.get_mvt(group,zoom,x,y)
            LOGGER.debug('created pbf')
            if response != None:
                self._cache.set_tile(group,x,y,zoom,response)
        return response
    


    
    
