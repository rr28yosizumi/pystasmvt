from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import os
from pystasmvt import mvtsql

CONFIG={
    'groups':{
        'group_name':'',
        'layers':'layerpath.json'
    }
}

# 最大スケールレベル
MAX_SCALE_LEVEL=19

def get_layerconfig_from_json(file):
    """ JSONファイルの読み込み
    """
    config = None
    with open(file,'r') as stream:
        config = json.load(stream)
    return config

class MvtApplication(object):
    def __init__(self):
        self._GROUP_SQL_LIST= {}
        self._ENGINE=None
    
    def init_db_session(self,config):
        """ サーバ起動の事前準備処理

        設定ファイルの読み込み
        PREPAREの実行
        EXECUTE文のリスト作成

        """
        self._ENGINE = create_engine('postgresql://'+os.getenv('POSTGRES_USER','map')+':'+os.getenv('POSTGRES_PASSWORD','map')+'@'+os.getenv('POSTGRES_HOST','localhost')+':'+os.getenv('POSTGRES_PORT','5432')+'/'+os.getenv('POSTGRES_DB','gis_test2'),
        pool_size=20, max_overflow=0)

        for group in config['groups']:
            self._GROUP_SQL_LIST[group['name']]={}
            layers = get_layerconfig_from_json(group['file'])
            if not layers:
                return False
            for scale in range(MAX_SCALE_LEVEL):
                prepared = mvtsql.MvtSql(layers,scale,scale)
                if prepared:
                    self._GROUP_SQL_LIST[group['name']][scale] = prepared
        return True
    
    def get_mvt(self,group_name,zoom,x,y):
        """ ベクタータイルのバイナリを生成

        """
        try:								# Sanitize the inputs
            sani_zoom,sani_x,sani_y = int(zoom),int(x),int(y)
            del zoom,x,y
        except:
            print('suspicious')
            return 1

        if group_name not in self._GROUP_SQL_LIST.keys():
            return 1

        layergroup = self._GROUP_SQL_LIST[group_name]

        if sani_zoom not in layergroup.keys():
            return 1
        
        if not self._ENGINE:
            return 1

        DBSession = sessionmaker(bind=self._ENGINE)
        session = DBSession()
        final_query = layergroup[sani_zoom]
        try:
            return final_query.get_mvt_by_query(session,sani_x,sani_y,sani_zoom)
        except:
            # SQLに失敗した場合にロールバックしないとセッションをロックしてしまう。
            session.rollback()
            raise