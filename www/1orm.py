#coding=utf-8
import asyncio,logging

import aiomysql
#对logging.info的封装，方便输出sql语句
def log(sql,args=()):
	logging.info('SQL:%s' % sql)
#创建数据库连接池，可以从此连接池中获取数据库连接，，详情可见aiomysql的文档
async def create_pool(loop,**kw):
	logging.info('create database connection pool...')
	#声明了一个全局变量，需要了解
	global __pool
	#http://aiomysql.readthedocs.io/en/latest/examples.html#low-level-api
	__pool = await aiomysql.create_pool(
		host = kw.get('host','localhost'),
		port = kw.get('port',3306),
		user = kw['user'],
		password = kw['password'],
		db = kw['db'],
		#从数据库获得的数据以utf8的格式返回
		charset = kw.get('charset','utf8'),
		#是否自动提交事务，设置为True时，在增删数据库数据时，不需要再commit来提交事务
		autocommit = kw.get('autocommit',True),
		maxsize = kw.get('maxsize',10),
		minsize = kw.get('minsize',1),
		loop = loop
		)

#该协程封装的是查询事务，第一个参数为sql语句，第二个是sql语句中占位符的参数列表，第三个是要查询数据的数量
#通过select函数来执行SELECT语句，需要传入SQL语句和SQL参数
#SQL的占位符是？，MySQL的占位符是%s
#size=None时获取所有记录仪，指定值则最多获取指定值的记录
async def select(sql,args,size=None):
	log(sql,args)
	global __pool
	#获取数据库连接
	with __pool.acquire() as conn:
		#获取游标，默认游标返回的结果为元祖，每一项是另一个元祖，可以通过aiomysql.DictCursor指定元组的元素为字典
		cur = await conn.cursor(aiomysql.DictCursor)
		await cur.execute(sql.replace('?','%s'),args or ())
		if size:
			rs = await cur.fetchmany(size)
		else:
			rs = await cur.fetchall()
		await cur.close()
		logging.info('rows returned:%s' % len(rs))
		return rs

#定义一个通用的execute（）函数来执行INSERT UPDATE DELETE语句
async def execute(sql,args):
	log(sql)
	with __pool.acquire() as conn:
		try:
			cur =await conn.cursor()
			await cur.execute(sql.replace('?','%s'),args)
			affected = cur.rowcount
			await cur.close()
		except BaseException as e:
			raise
		return affected

def create_args_string(num):
	L=[]
	for n in range(num):
		L.append('?')
	return ','.join(L)

class Field(object):

	def __init__(self,name,column_type,primary_key,default):
		self.name=name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default=default

	def __str__(self):
		return '<%s,%s:%s>' %(self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):
	def __init__(self,name=None,default=False):
		super().__init__(name,'boolean',False,default)

class IntergerField(Field):
	def __init__(self,name=None,primary_key=False,default=0):
		super().__init__(name,'bigint',primary_key,default)

class FloatField(Field):
	def __init__(self,name=None,primary_key=False,default=0.0):
		super().__init__(name,'real',primary_key,default)

class TextField(Field):
	def __init__(self,name=None,default=None):
		super().__init__(name,'text',False,default)

class ModelMetaclass(type):
	def __new__(cls,name,bases,attrs):
		if name=='model':
			return type.__new__(cls,name,bases,attrs)
		tableName=attrs.get('__table__',None) or name
		logging.info('found model:%s (table:%s)' % (name,tableName))
		mappings=dict()
		fields=[]
		primaryKey=None
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info(' found mapping:%s ==>%s' %(k,v))
				mappings[k]=v
				if v.primary_key:
					#找到主键
					if primaryKey:
						print('hello')
						#raise StandardError('Duplicate primary key for field:%s' %k)
					primaryKey=k
				else:
					fields.append(k)
		if not primaryKey:
			print('world')
			#raise StandardError('Primary key not found.')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields=list(map(lambda f:'`%s`' % f,fields))
		attrs['__mappings__']=mappings 
		attrs['__table__']=tableName
		attrs['__primary_key__']=primaryKey
		attrs['__fields__']=fields
		attrs['__select__']='select `%s`,%s from `%s`' %(primaryKey,', '.join(escaped_fields),tableName)
		attrs['__insert__']='insert into `%s`(%s,`%s`)values(%s)' %(tableName,', '.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
		attrs['__update__']='update `%s` set %s where `%s`=?' %(tableName,', '.join(map(lambda f:'`%s`=?' %(mappings.get(f).name or f),fields)),primaryKey)
		attrs['__delete__']='delete from `%s` where `%s`=?' %(tableName,primaryKey)
		return type.__new__(cls,name,bases,attrs)

class Model(dict,metaclass=ModelMetaclass):

	def __init__(self,**kw):
		super(Model,self).__init__(**kw)

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" %key)

	def __setattr__(self,key,value):
		self[key]=value

	def getValue(self,key):
		return getattr(self,key,None)

	def getValueOrDefault(self,key):
		value=getattr(self,key,None)
		if value is None:
			field=self.__mappings__[key]
			if field.default is not None:
				value=field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s:%s' %(key,str(value)))
				setattr(self,key,value)
		return value


	@classmethod
	async def findAll(cls,where=None,args=None,**kw):
		#' find objects by where clause.'
		sql=[cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args=[]
		orderBy=kw.get('orderBy',None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit',None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit,int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit,tuple) and len(limit) ==2:
				sql.append('?,?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value:%s' %str(limit))
			rs=await select(''.join(sql),args)
			return [cls(**r) for r in rs]

	@classmethod
	async def find(cls,pk):
		'fing object by primary key.'
		rs=await select('%s where `%s`=?' %(cls.__select__,cls.__primary_key__),[pk],1)
		if len(rs) ==0:
			return None
		return cls(**rs[0])

	async def save(self):
		args=list(map(self.getValueOrDefault,self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows=await execute(self.__insert__,args)
		if rows !=1:
			logging.warn('failed to insert record:affected rows:%s' %rows)

	async def update(self):
		args=list(map(self.getValue,self__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows=await execute(self.__update__,args)
		if rows !=1:
			logging.warn('failed to update by primary key:affected rows:%s' % rows)

	async def remove(self):
		args=[self.getValue(self.__primary_key__)]
		rows=await execute(self.__delete__,args)
		if rows !=1:
			logging.warn('falied to remove by primary key:affected rows:%s' %rows)





















