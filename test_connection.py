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
        # 获取列名 (注意: 在新版本的 nebula3-python API 中使用 keys() 而不是 column_names())
        column_names = resp.keys()
        print("列名:", column_names)
        
        # 遍历每一行结果
        # 遍历每一行结果
        for row in resp.rows():
            # NebulaGraph v3.8.3 API 中，row 是特殊的 Row 对象
            try:
                # 尝试直接通过索引访问
                player_id = row[0]
                player_name = row[1]
                print(f"球员 ID: {player_id}")
                print(f"球员名字: {player_name}")
                if str(player_name) == "Tim Duncan":
                    print("找到 Tim Duncan!")
            except (IndexError, TypeError) as e:
                print(f"无法解析行数据: {row}, 错误: {e}")
                # 尝试获取更多信息
                print(f"行数据类型: {type(row)}")
                print(f"行数据内容: {repr(row)}")
    else:
        print("查询失败:", resp.error_msg())
    
    # 目标3: 查询球员的关注关系
    print("\n==== 查询球员的关注关系 ====")
    query = 'MATCH (v:player{name:"Tim Duncan"})-[e:follows]->(friend) RETURN friend'
    resp = session.execute(query)
    
    if resp.is_succeeded():
        print("查询成功!")
        column_names = resp.keys()
        print("列名:", column_names)
        
        print("Tim Duncan 关注的球员:")
        for i, row in enumerate(resp.rows()):
            # 在 NebulaGraph v3.8.3 API 中，row 已经是一个列表
            if row:
                print(f"{i+1}. 好友信息: {row}")
                # 尝试提取好友名字 (如果有)
                if len(row) > 0:
                    friend_info = row[0]
                    print(f"   详细信息: {friend_info}")
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
        column_names = resp.keys()
        print("列名:", [name.decode('utf-8') if isinstance(name, bytes) else name for name in column_names])
        
        # 打印查询结果
        for row in resp.rows():
            if len(row) >= 2:
                relation_type = str(row[0])  # 使用 str() 替代 .as_string()
                player_name = str(row[1])
                print(f"{relation_type}: {player_name}")
            else:
                print(f"行数据: {row}")
    else:
        print("查询失败:", resp.error_msg())

# 关闭连接池
connection_pool.close()
