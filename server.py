import tornado.ioloop
import tornado.web
import io
import os
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pystasmvt import mvtsql

# 設定ファイルの格納先
LAYERCONFIG_PATH='./layerconfig.json'

# 最大スケールレベル
MAX_SCALE_LEVEL=191

# EXECUTE文格納用
_SCALE_SQL_LIST={}

# セッションの格納先

_ENGINE = None

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
    if not layers:
        return False

    for scale in range(MAX_SCALE_LEVEL):
        prepared = mvtsql.MvtSql(layers,scale,scale)
        if prepared:
            _SCALE_SQL_LIST[scale] = prepared
    return True

def get_mvt(zoom,x,y):
    """ ベクタータイルのバイナリを生成

    """
    try:								# Sanitize the inputs
        sani_zoom,sani_x,sani_y = int(zoom),int(x),int(y)
        del zoom,x,y
    except:
        print('suspicious')
        return 1

    if sani_zoom not in _SCALE_SQL_LIST.keys():
        return 1

    DBSession = sessionmaker(bind=_ENGINE)
    session = DBSession()
    final_query = _SCALE_SQL_LIST[sani_zoom]
    try:
        return final_query.get_mvt_by_query(session,sani_x,sani_y,sani_zoom)
    except:
        # SQLに失敗した場合にロールバックしないとセッションをロックしてしまう。
        session.rollback()
        raise

class GetTile(tornado.web.RequestHandler):
    def get(self, zoom,x,y):
        self.set_header("Content-Type", "application/x-protobuf")
        self.set_header("Content-Disposition", "attachment")
        self.set_header("Access-Control-Allow-Origin", "*")
        response = get_mvt(zoom,x,y)
        if response != 1:
            self.write(response)

def main():
    if not init_db_session():
        print('Failed initialize')
        return
    
    application = tornado.web.Application([(r"/tiles/([0-9]+)/([0-9]+)/([0-9]+).pbf", GetTile)])
    print("Postserve started..")
    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    # コネクションの作成
    _ENGINE = create_engine('postgresql://'+os.getenv('POSTGRES_USER','map')+':'+os.getenv('POSTGRES_PASSWORD','map')+'@'+os.getenv('POSTGRES_HOST','localhost')+':'+os.getenv('POSTGRES_PORT','5432')+'/'+os.getenv('POSTGRES_DB','gis_test2'),
    pool_size=20, max_overflow=0)
    
    main()
