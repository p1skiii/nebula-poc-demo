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
        exit(1)  # 如果切换图空间失败，终止程序
    # 目标2: 查询球员属性（比如 Tim Duncan 的 age）
    print("\n==== 查询球员属性 ====")
    query = 'MATCH (v:player) RETURN id(v), v.name'
    resp = session.execute(query)
    
    if resp.is_succeeded():
        # 解析返回结果获取球员属性
        print("查询成功!")
        # 获取列名
        column_names = resp.column_names()
        print("列名:", column_names)
        
        # 遍历每一行结果
        for record in resp.records():
            # 获取节点值 (v 列)
            node_val = record.values()[0]
            # 获取节点的所有属性
            props = node_val.as_node().properties()
            print(f"球员 ID: {node_val.as_node().vid().as_string()}")
            print(f"球员属性: {props}")
            # 特别打印 age 属性
            if 'age' in props:
                print(f"Tim Duncan 的年龄: {props['age'].as_int()}")
    else:
        print("查询失败:", resp.error_msg())
    
    # 目标3: 查询球员的关注关系
    print("\n==== 查询球员的关注关系 ====")
    query = 'MATCH (v:player{name:"Tim Duncan"})-[e:follows]->(friend) RETURN friend'
    resp = session.execute(query)
    
    if resp.is_succeeded():
        print("查询成功!")
        column_names = resp.column_names()
        print("列名:", column_names)
        
        print("Tim Duncan 关注的球员:")
        for i, record in enumerate(resp.records()):
            # 获取好友节点
            friend_node = record.values()[0]
            # 获取好友 ID
            friend_id = friend_node.as_node().vid().as_string()
            # 获取好友属性
            friend_props = friend_node.as_node().properties()
            friend_name = friend_props.get('name', '未知').as_string() if 'name' in friend_props else '未知'
            print(f"{i+1}. 好友ID: {friend_id}, 姓名: {friend_name}")
    else:
        print("查询失败:", resp.error_msg())

    # 额外示例：查询双向关系（谁关注了 Tim Duncan，以及 Tim Duncan 关注了谁）
    print("\n==== 双向关系查询示例 ====")
    query = '''
    MATCH (v:player{name:"Tim Duncan"})-[e:follows]->(friend:player) 
    RETURN "Tim Duncan关注" AS 关系类型, friend.name AS 球员
    UNION
    MATCH (friend:player)-[e:follows]->(v:player{name:"Tim Duncan"}) 
    RETURN "关注Tim Duncan" AS 关系类型, friend.name AS 球员
    '''
    resp = session.execute(query)
    
    if resp.is_succeeded():
        # 获取列名
        column_names = resp.column_names()
        print("列名:", [name.decode('utf-8') for name in column_names])
        
        # 打印查询结果
        for record in resp.records():
            values = record.values()
            relation_type = values[0].as_string()
            player_name = values[1].as_string()
            print(f"{relation_type}: {player_name}")
    else:
        print("查询失败:", resp.error_msg())

# 关闭连接池
connection_pool.close()
