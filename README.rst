这是一个超轻型sql映射为键值对的框架，支持自动管理连接池和清理超时连接池
与mysql保持心跳的周期为30秒
连接池将会保持在10个作为稳定链接
可以通过修改anduin.dbserver.data_manager.data_manager.min_keep_connection控制系统维持的连接池数量

依赖pymysql
通过from anduin.server import Data

Data.find
Data.selet
Data.update
Data.insert
Data.delete
进行操作

Data.query可以直接执行sql语句