import tornado.ioloop
import tornado.web
import io
import os
import json

from pystasmvt import mvtcreator

# 設定ファイルの格納先
LAYERCONFIG_PATH='./mvtsetting.json'

# セッションの格納先

_MVTCREATOR=None

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
            'host':os.getenv('POSTGRES_HOST','r24119'),
            'port':os.getenv('POSTGRES_PORT','5432'),
            'dbname':os.getenv('POSTGRES_DB','gis_test2')
        },
        'groups':layers['groups']
    }
    global _MVTCREATOR
    _MVTCREATOR = mvtcreator.MvtCreator()
    _MVTCREATOR.init_db_session(config)

    return True

class GetTile(tornado.web.RequestHandler):
    def get(self,group, zoom,x,y):
        global _MVTCREATOR
        self.set_header("Content-Type", "application/x-protobuf")
        self.set_header("Content-Disposition", "attachment")
        self.set_header("Access-Control-Allow-Origin", "*")
        response = _MVTCREATOR.get_mvt(group,zoom,x,y)
        if response != 1:
            self.write(response)

def main():
    if not init_db_session():
        print('Failed initialize')
        return
    
    application = tornado.web.Application([(r"/tiles/([a-z_]+)/([0-9]+)/([0-9]+)/([0-9]+).pbf", GetTile)])
    print("Postserve started..")
    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    # コネクションの作成 
    main()
