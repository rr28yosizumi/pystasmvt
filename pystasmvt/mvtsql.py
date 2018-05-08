
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import io
import sys
import itertools

import logging
LOGGER = logging.getLogger(__name__)

_STASMVT_TEMPLATE = (
        "SELECT ST_AsMVT(t_mvt, '{layername}', {extent}, 'mvt_geom') "
        "FROM ("
        "    SELECT"
        "        {attr_col}"
        "        ST_AsMVTGeom("
        "            {geofunc},"
        "            st_makeenvelope("
        "               tile2lon({minx},{scale}),"
        "               tile2lat({miny},{scale}),"
        "               tile2lon({maxx},{scale}),"
        "               tile2lat({maxy},{scale}),"
        "               {srid}),"
        "            {extent},"
        "            {buffer},"
        "            {clip}) AS mvt_geom"
        "    from ("
        "{origin_table}"
        "    ) a {group_by_attr_col}"
        ") as t_mvt")

def generate_queris(layers,scale_level,extent=4096,buffer=256,clip=True):
    queries = []
    for layer in layers:
        if scale_level in layer['enable_scale']:
            queries.append(generate_sql(layer,extent,buffer,clip))
    
    if not queries:
        return ''

    query =  " UNION ALL ".join(queries) + ";"
    return query

def generate_namespace_table(namespacelist):
    '''
    複数のネームスペースに対応
    '''
    sql =[]
    for i in namespacelist:
        sql.append((
            " SELECT {attr_col}(ST_Dump({geometry_col})).geom "
            " FROM "+i+'.'+"{tablename} "
            " WHERE {geometry_col} && st_makeenvelope("
            "                               tile2lon({minx},{scale}),"
            "                               tile2lat({miny},{scale}),"
            "                               tile2lon({maxx},{scale}),"
            "                               tile2lat({maxy},{scale}),"
            "                               {srid})"
            "       {where}"
        ))

    return ' UNION ALL '.join(sql)


def generate_sql(layer,extent=4096,buffer=256,clip=True):
    """ SQLの作成

    """
    geofunc = ''
    if layer['geotype'] == 'line':
        geofunc = 'ST_LineMerge(ST_Collect(geom))'
    elif layer['geotype'] == 'polygon':
        geofunc = 'ST_Union(geom)'
    else:
        geofunc = 'ST_Collect(geom)'

    origin_table = (
            " SELECT {attr_col}(ST_Dump({geometry_col})).geom "
            " FROM {tablename} "
            " WHERE {geometry_col} && st_makeenvelope("
            "                               tile2lon({minx},{scale}),"
            "                               tile2lat({miny},{scale}),"
            "                               tile2lon({maxx},{scale}),"
            "                               tile2lat({maxy},{scale}),"
            "                               {srid})"
            "       {where}"
        )

    if'namespace' in layer.keys():
        origin_table = generate_namespace_table(layer['namespace'])

    return _STASMVT_TEMPLATE.replace('{origin_table}',origin_table).format(
        **{
            'layername': layer['layername'],
            'tablename': layer['tablename'],
            'attr_col': layer['attr_col']+',' if layer['attr_col'] else '',
            'group_by_attr_col':'GROUP BY '+layer['attr_col'] if layer['attr_col'] else '',
            'geometry_col': layer['geometry_col'],
            'srid': layer['srid'],
            'minx': '$2',
            'miny': '$3',
            'maxx': '$2+1',
            'maxy': '$3+1',
            'scale': '$1',
            'where': ' AND '+layer['where'] if layer['where'] else '',
            'geofunc': geofunc,
            'extent':extent,
            'buffer':buffer,
            'clip':'true' if clip else 'false'
        }
    )

def get_mvt(session,sql,zoom,x,y):
    """ ベクタータイルのバイナリを生成

    """
    try:
        #print('sql:'+sql)
        response = list(session.execute(sql))
    except:
        # SQLに失敗した場合にロールバックしないとセッションをロックしてしまう。
        session.rollback()
        raise

    layers = filter(None,list(itertools.chain.from_iterable(response)))
    final_tile = b''
    for layer in layers:
        final_tile = final_tile + io.BytesIO(layer).getvalue() 
    return final_tile

class MvtSql(object):
    def __init__(self,layers,scale_level,func_name,time_out=3000,extent=4096,buffer=256,clip=True):
        self._scale_level=scale_level
        self._func_name=func_name
        self._time_out = time_out
        result = generate_queris(layers,scale_level,extent,buffer,clip)
        self._queri = result

    def _statement_timeout(self,session):
        try:
            session.execute('SET statement_timeout TO {0};'.format(self._time_out))
        except:
            session.rollback()
            raise
    
    def get_query(self,x,y,z):
        return self._queri.replace('$2+1',str(x+1)).replace('$3+1',str(y+1)).replace('$1',str(z)).replace('$2',str(x)).replace('$3',str(y))
    
    def get_prepare(self):
        prepared = "PREPARE gettile_{0}(integer, integer, integer) AS ".format(self._func_name)
        return prepared + self._queri

    def get_execute(self,x,y,z):
        execute = "EXECUTE gettile_{0}".format(self._func_name)
        execute += "({0},{1},{2});"
        return execute.format(x,y,z)
    
    def get_mvt_by_query(self,session,x,y,z):
        try:								# Sanitize the inputs
            sani_zoom,sani_x,sani_y = int(z),int(x),int(y)
        except:
            LOGGER.error('suspicious')
            return None
        
        self._statement_timeout(session)
        final_query = self.get_query(sani_x,sani_y,sani_zoom)
        return get_mvt(session,final_query,z,x,y)
    
    def get_mvt_by_execute(self,session,x,y,z):
        try:								# Sanitize the inputs
            sani_zoom,sani_x,sani_y = int(z),int(x),int(y)
        except:
            LOGGER.error('suspicious')
            return None
            
        self._statement_timeout(session)
        final_query = self.get_execute(sani_x,sani_y,sani_zoom)
        return get_mvt(session,final_query,z,x,y)
    
    def is_query(self):
        return True if self._queri else False 


    


