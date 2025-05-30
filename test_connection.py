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
        # row 是一个 Row 对象，需要使用 get_value() 方法获取值
        # print(row.values[0])
        player_name = row.values[0].get_sVal().decode('utf-8')
        player_age = row.values[1].get_iVal()
        print(f"姓名: {player_name}, 年龄: {player_age}")

    print(20* "---")

    gql_go_with_prop = 'GO FROM "player100" OVER follow YIELD dst(edge) AS friend_id, properties(edge).degree AS follow_degree;'
    
    resp = session.execute(gql_go_with_prop)
    print(resp)
    for i in resp.rows():
        friend_id = i.values[0].get_sVal().decode('utf-8')
        follow_degree = i.values[1].get_iVal()
        print(f"好友 ID: {friend_id}, 关系强度: {follow_degree}")
    print(20* "---")

    query = '''
    MATCH p=(v1:player)-[:follow]->(v2:player) 
    WHERE id(v1)=="player100" 
    RETURN p 
    LIMIT 5;
    '''
    resp = session.execute(query)
    # print(resp)
    for row in resp.rows():
        path = row.values[0]
        # print(path)
        path2 = path.get_pVal()
        #print(path2)
        src_vertex = path2.src
        # print(src_vertex)
        player1_vid = src_vertex.vid.get_sVal().decode('utf-8')
        # print(player1_vid)
        player1_tag = src_vertex.tags[0]  
        player1_props = player1_tag.props  
        player1_name = player1_props[b'name'].get_sVal().decode('utf-8')
        player1_age = player1_props[b'age'].get_iVal()
        
        step = path2.steps[0]
        #print(step)
        dst_vertex = step.dst
        #print(dst_vertex)
        edge_id = dst_vertex.vid.get_sVal().decode('utf-8')
        # print(edge)
        edge_props = dst_vertex.tags[0].props
        # print(edge_props)

        edge_degree = step.props[b'degree'].get_iVal()
        print(edge_degree)
        edge_name = edge_props[b'name'].get_sVal().decode('utf-8')
        
        print(edge_name)
        player2_vid = dst_vertex.vid.get_sVal().decode('utf-8')
        # print(player2_vid)
        dst_vertex = step.dst
        player2_vid = dst_vertex.vid.get_sVal().decode('utf-8')
        player2_tag = dst_vertex.tags[0]
        player2_props = player2_tag.props
        player2_name = player2_props[b'name'].get_sVal().decode('utf-8')
        player2_age = player2_props[b'age'].get_iVal()
        print(f"路径: {player1_name}({player1_vid}) --[{edge_name}:{edge_degree}]--> {player2_name}({player2_vid})")
        print(f"球员1: {player1_name}, 年龄: {player1_age}")
        print(f"球员2: {player2_name}, 年龄: {player2_age}")#player1_props = player1_vertex.get_props()
        #player1_name = player1_props[b'name'].get_sVal().decode('utf-8')
        #player1_age = player1_props[b'age'].get_iVal()
    print(20* "---")

        

# 关闭连接池
connection_pool.close()
