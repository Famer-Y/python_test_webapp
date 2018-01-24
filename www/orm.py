# -*- coding: utf-8 -*-
#
import asyncio, logging, aiomysql

# 打印sql语句
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

# 创建数据库连接池
async def create_pool(loop, **kwargs):  # **kwargs 关键字参数
    logging.info('Create database connection pool...')  # 保存日志信息
    global __pool  # global关键字：用于定义全局变量
    __pool = await aiomysql.create_pool(
        host=kwargs.get('host', 'localhost'),       # 数据库所在的地址
        port=kwargs.get('port', 3306),              # 数据库所使用的端口
        user=kwargs['user'],                        # 数据库连接使用的用户名
        password=kwargs['password'],                # 数据库连接使用的密码
        db=kwargs['db'],                            # 所连接的数据库名
        charset=kwargs.get('charset', 'utf8'),     # 连接数据库使用的字符集
        autocommit=kwargs.get('autocommit', True),  # 设置自动提交
        maxsize=kwargs.get('maxsize', 10),
        minsize=kwargs.get('minsize', 1),
        loop=loop
    )

# 查询语句执行的基本函数
async def select(sql, args, size=None):
    log(sql, args)
    global __pool  # 引用全局变量
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

# 删除，添加，修改语句所执行的基本函数
async def execute(sql, args, autocommit=True):
    log(sql)
    global __pool
    async with __pool.get() as conn:    # 获取数据库连接
        if not autocommit:              # 如果不是自动提交
            await conn.begin()          # 开始事务
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:  # 为sql语句的执行做准备
                await cur.execute(sql.replace('?', '%s'), args)  # replace()方法用于替换字符串
                affected = cur.rowcount
            if not autocommit:
                conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

# 定义基本字段
class Field(object):
    # 给类定义一些基本属性
    def __init__(self, name, data_type, primary_key, default):
        self.name = name
        self.data_type = data_type
        self.primary_key = primary_key
        self.default = default

    # __str__()方法：用于定制类，可以自定义返回的字符串，便于了解类的内容
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.data_type, self.name)

# 定义StringField，用于映射数据库中字符类型：例如；char,varchar等
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(20)'):
        super().__init__(name, ddl, primary_key, default)

# IntegerField，用于映射数据库中整数类型：例如；int,bigint等
class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0, ddl='bigint'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0, ddl='real'):
        super().__init__(name, ddl, primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('Found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        fields = []
        primarykey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('Found mapping: %s --> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primarykey:
                        raise Exception('Duplicate primary for field: %s' % k)
                    primarykey = k
                else:
                    fields.append(k)
        if not primarykey:
            raise Exception('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primarykey
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primarykey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primarykey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primarykey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primarykey)
        return type.__new__(cls, name, bases, attrs)

# 定义Model基类
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % item)

    def __setattr__(self, key, value):
        self[key] = value

    # getattr(object, name[, default])
    # object - - 对象。
    # name - - 字符串，对象属性。
    # default - - 默认返回值，如果不提供该参数，在没有对应属性时，将触发AttributeError。
    # 返回对象属性值。
    def getValue(self, key):  # 用来获取属性值
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                # callable() 函数用于检查一个对象是否是可调用的。如果返回True，object仍然可能调用失败；但如果返回False，调用对象ojbect绝对不会成功
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # 查询所有的信息
    @classmethod
    async def findAll(cls, where=None, args=None, **kwargs):
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args =[]
        orderBy = kwargs.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kwargs.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and 2 == len(limit):
                sql.append('?, ?')
                args.append(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return[cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # 向数据库中插入数据
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('Failed to insert record: affected rows: %s' % rows)

    # 更新数据库中的数据
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('Failed to update by primary key: affected rows: %s' % rows)

    # 删除数据库中的数据
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('Failed to remove by primary key: affected rows: %s' % rows)
