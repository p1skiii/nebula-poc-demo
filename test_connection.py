from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

# 配置连接池
config = Config()
connection_pool = ConnectionPool()

# 初始化连接（地址需与 Docker 映射的端口一致）
success = connection_pool.init([('127.0.0.1', 9669)], config)
print("连接池初始化成功？", success)

# 执行测试查询
with connection_pool.session_context('root', 'nebula') as session:

    resp = session.execute('USE basketballplayer')
    if resp.is_succeeded():
        print("成功切换到 nba 图空间")
    else:
        print("切换图空间失败:", resp.error_msg())
        exit(1)  
    """
    query = 'MATCH (v:player) RETURN id(v), v.name'
    resp = session.execute(query)
    print(resp)
    """
    gql_fetch_prop = 'FETCH PROP ON player "player100", "player101" YIELD properties(vertex).name, properties(vertex).age;'
    resp = session.execute(gql_fetch_prop)
    print(resp)

"""
    print(20* "---")

    gql_match_prop = 'MATCH (v:player) WHERE id(v) IN ["player100", "player101"] RETURN id(v) AS player_id, v.name AS player_name, v.age AS player_age;'
    resp = session.execute(gql_match_prop)
    print(resp)
    print(20* "---")

    gql_go = 'GO FROM "player100" OVER follow YIELD dst(edge) AS friend_id;'
    resp = session.execute(gql_go)
    print(resp)
    print(20* "---")

    gql_go_with_prop = 'GO FROM "player100" OVER follow YIELD dst(edge) AS friend_id, properties(edge).degree AS follow_degree;'
    resp = session.execute(gql_go_with_prop)
    print(resp)
    print(20* "---")
"""
# 关闭连接池
connection_pool.close()
