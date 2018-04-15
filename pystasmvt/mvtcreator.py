from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import os
from pystasmvt import mvtsql

#設定テンプレート
CONFIG={
    'connection':{
        'user':'',
        'password':'',
        'host':'',
        'port':'',
        'dbname':''
    },
    'groups':[
        {
            'group_name':'',
            'layers':'layerpath.json'
        }
    ]
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

class MvtCreator(object):
    def __init__(self):
        self._GROUP_SQL_LIST= {}
        self._ENGINE=None
    
    def init_db_session(self,config):
        """ 事前準備処理

        設定ファイルの読み込み
        PREPAREの実行
        EXECUTE文のリスト作成

        """

        p_user= config['connection']['user']
        p_pw = config['connection']['password']
        p_host = config['connection']['host']
        p_port = config['connection']['port']
        p_dbname = config['connection']['dbname']

        #self._ENGINE = create_engine('postgresql://'+os.getenv('POSTGRES_USER','map')+':'+os.getenv('POSTGRES_PASSWORD','map')+'@'+os.getenv('POSTGRES_HOST','localhost')+':'+os.getenv('POSTGRES_PORT','5432')+'/'+os.getenv('POSTGRES_DB','gis_test2'),
        self._ENGINE = create_engine('postgresql://'+p_user+':'+p_pw+'@'+p_host+':'+p_port+'/'+p_dbname,pool_size=20, max_overflow=0)

        for group in config['groups']:
            if group['group_name'] not in self._GROUP_SQL_LIST.keys():
                self._GROUP_SQL_LIST[group['group_name']]={}
    
            layers = group['layers']
            if not layers:
                return False
            for scale in range(MAX_SCALE_LEVEL):
                prepared = mvtsql.MvtSql(layers,scale,scale)
                if prepared:
                    self._GROUP_SQL_LIST[group['group_name']][scale] = prepared
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
        if not final_query.is_query():
            return 1
        try:
            return final_query.get_mvt_by_query(session,sani_x,sani_y,sani_zoom)
        except:
            # SQLに失敗した場合にロールバックしないとセッションをロックしてしまう。
            session.rollback()
            raise