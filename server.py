import tornado.ioloop
import tornado.web

from pystasmvt import mvtserver

import logging
LOGGER = logging.getLogger(__name__)

# 設定ファイルの格納先
LAYERCONFIG_PATH='./mvtsetting.json'

#ポート
PORT=8080

# キャッシュ
CACHE_PATH='./'

# キャッシュ使用の有無
CACHE_USE=True

_MVTCREATESERVER=mvtserver.MvtCreateServer(LAYERCONFIG_PATH,CACHE_USE,CACHE_PATH)

def main():
    if not _MVTCREATESERVER:
        LOGGER.debug('Failed initialize')
        return
    
    application = tornado.web.Application([_MVTCREATESERVER.get_GetMvtTile_Application()])
    LOGGER.info("Pystasmve_serve started..")
    application.listen(PORT)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # コネクションの作成 
    main()
