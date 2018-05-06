from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import os
import logging
from pystasmvt import mvtsql

LOGGER = logging.getLogger(__name__)

#設定テンプレート
CONFIG={
    'connection':{
        'user':'',#require
        'password':'',#require
        'host':'',#require
        'port':'',#require
        'dbname':''#require
    },
    'groups':[
        {
            'group_name':'',#require
            'layers':{#require
                'layername':'',#require
                'namespace':[],#option
                'tablename':'',#require
                'attr_col':'',#require
                'where':'',#require
                'geometry_col':'',#require
                'srid':4236,#require
                'geotype':'',#require
                'enable_scale':[]#require
            },
            'time_out':''#option
        }
    ]
}

# 最大スケールレベル
MAX_SCALE_LEVEL=19

DEFAULT_TIME_OUT=3000#ms

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
                time_out = DEFAULT_TIME_OUT
                if 'time_out' in group.keys():
                    time_out = group['time_out']
                prepared = mvtsql.MvtSql(layers,scale,scale,time_out)
                if prepared:
                    self._GROUP_SQL_LIST[group['group_name']][scale] = prepared
        return True
    
    def get_mvt(self,group_name,zoom,x,y):
        """ ベクタータイルのバイナリを生成

        """
        try:								# Sanitize the inputs
            sani_zoom,sani_x,sani_y = int(zoom),int(x),int(y)
            del zoom,x,y
        except Exception as e:
            LOGGER.error('suspicious')
            LOGGER.exception(e)
            return None

        if group_name not in self._GROUP_SQL_LIST.keys():
            return None

        layergroup = self._GROUP_SQL_LIST[group_name]

        if sani_zoom not in layergroup.keys():
            return None
        
        if not self._ENGINE:
            return None

        DBSession = sessionmaker(bind=self._ENGINE)
        session = DBSession()
        final_query = layergroup[sani_zoom]
        if not final_query.is_query():
            return None
        try:
            return final_query.get_mvt_by_query(session,sani_x,sani_y,sani_zoom)
        except Exception as e:
            LOGGER.error("get_mvt")
            LOGGER.exception(e)
            raise
        finally:
            session.connection().close()