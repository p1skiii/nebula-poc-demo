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
    # 选择图空间
    session.execute('USE basketballplayer')
    
    # 执行简单查询
    resp = session.execute('SHOW TAGS;')
    if resp.is_succeeded():
        print("查询结果：", resp.rows())
    else:
        print("错误信息：", resp.error_msg())

# 关闭连接池
connection_pool.close()
