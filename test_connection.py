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
    编写 Python 代码遍历 ResultSet，提取并打印出每个球员的 ID、姓名和年龄。
    """
    # 这里的 resp 是一个 ResultSet 对象
    for row in resp.rows():
        # row 是一个 Row 对象，包含了查询结果的一行数据
        # 通过 row.values() 方法获取该行的所有值
        values = row.values()
        if values is None:
            print(resp.rows)
            exit(1)
        # 打印每个球员的 ID、姓名和年龄
        print(f"球员 ID: {values[0]}, 姓名: {values[1]}, 年龄: {values[2]}")

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
