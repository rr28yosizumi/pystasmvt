import tornado.ioloop
import tornado.web
from tornado import gen
import io
import os
import json
import concurrent.futures

from pystasmvt import mvtcreator
from pystasmvt import mvtcache

# 設定ファイルの格納先
LAYERCONFIG_PATH='./mvtsetting1.json'

# キャッシュ
CACHE_PATH='./'

# キャッシュ使用の有無
CACHE_USE=True

#スレッド
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=3)

#タイル生成
_MVTCREATOR=None

#キャッシュの生成、保存、取得
_CACHE=None

def get_layerconfig_from_json(file):
    """ JSONファイルの読み込み
    """
    config = None
    with open(file,'r') as stream:
        config = json.load(stream)
    return config
    

def init_db_session():
    """ サーバ起動の事前準備処理

    設定ファイルの読み込み
    PREPAREの実行
    EXECUTE文のリスト作成

    """
    layers = get_layerconfig_from_json(LAYERCONFIG_PATH)

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
    global _MVTCREATOR
    global _CACHE
    _MVTCREATOR = mvtcreator.MvtCreator()
    _MVTCREATOR.init_db_session(config)

    if CACHE_USE:
        _CACHE = mvtcache.MbtilesCache(CACHE_PATH)
        #_CACHE = mvtcache.FileCache(CACHE_PATH)

    return True

class GetTile(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self,group, zoom,x,y):

        #https://stackoverflow.com/questions/11679040/using-gen-task-with-tornado-for-a-simple-function

        #https://gist.github.com/lbolla/3826189

        self.set_header("Content-Type", "application/x-protobuf")
        self.set_header("Content-Disposition", "attachment")
        self.set_header("Access-Control-Allow-Origin", "*")
        print('{0}/{1}/{2}/{3}'.format(group,zoom,x,y))

        #イベントループを作るだけで非同期処理を自動的にしてくれるわけではない。
        response = yield gen.Task(self.fatch_tile_data,group,zoom,x,y)
        
        if response != None:
            self.write(response)
        self.finish()
    
    def fatch_tile_data(self,group,zoom,x,y,callback):
        
        def fatch_func():
            response=None
            if CACHE_USE:
                response = self._used_cache(group,zoom,x,y)
            else:
                response = self._unused_cache(group,zoom,x,y)
            #コールバック関数に戻り値を渡すことで、yieldの戻り値にしてくれる。
            callback(response)

        EXECUTOR.submit(fatch_func)

    
    def _unused_cache(self,group,zoom,x,y):
        global _MVTCREATOR
        response = _MVTCREATOR.get_mvt(group,zoom,x,y)
        print('created pbf')
        return response
        
    
    def _used_cache(self,group,zoom,x,y):
        global _MVTCREATOR
        global _CACHE
        response = _CACHE.get_tile(group,x,y,zoom)
        if not response:
            response = _MVTCREATOR.get_mvt(group,zoom,x,y)
            print('created pbf')
            if response != None:
                _CACHE.set_tile(group,x,y,zoom,response)
        return response

def main():
    if not init_db_session():
        print('Failed initialize')
        return
    
    application = tornado.web.Application([(r"/tiles/([a-z_]+)/([0-9]+)/([0-9]+)/([0-9]+).pbf", GetTile)])
    print("Pystasmve_serve started..")
    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    # コネクションの作成 
    main()
