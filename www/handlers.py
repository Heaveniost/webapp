# -*- coding: utf-8 -*-

__author__ = 'Heaven'

' url handlers '

import re, time, json, logging, hashlib, base64, asyncio
import markdown2
from aiohttp import web
from coroweb import get, post
from apis import Page, APIError, APIValueError, APIResourceNotFoundError
from models import User, Comment, Blog, next_id
from config import configs 


COOKIE_NAME = 'awesession'
_COOKIE_KEY = configs.session.secret


# 检查是否是管理员 由于是个人博客 只有管理员能发文 普通用户只有评论的权限
def check_admin(request):
    if request.__user__ is None:  # or not request.__user__.admin:
        raise APIPermissionError() # 不需要Import吗


# 这一步的作用是什么？
def get_page_index(page_str):
    p = 1 
    try:
        p = int(page_str)
    except ValueError as e:
        pass
    if p < 1:
        p = 1
    return p


# 计算cookie值
def user2cookie(user, max_age): 
	'''
	Generate cookie str by user.
	'''
	# build cookie string by: id-expires-sha1
	expires = str(int(time.time() + max_age))
	s = '%s-%s-%s-%s' % (user.id, user.passwd, expires, _COOKIE_KEY)
	L = [user.id, expires, hashlib.sha1(s.encode('utf-8')).hexdigest()]
	return '-'.join(L)

# ??
def text2html(text):
    lines = map(lambda s: '<p>%s</p>' % s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'), filter(lambda s: s.strip() != '', text.split('\n')))
    return ''.join(lines)

#根据cookie值验证身份
@asyncio.coroutine
def cookie2user(cookie_str): 
	'''
	Parse cookie and load user if cookie is valid.
	'''
	if not cookie_str:
		return None
	try:
		L = cookie_str.split('-')
		if len(L) != 3:
			return None
		uid, expires, sha1 = L
		if int(expires) < time.time():
			return None
		user = yield from User.find(uid)
		if user is None:
			return None
		s = '%s-%s-%s-%s' % (uid, user.passwd, expires, _COOKIE_KEY)
		if sha1 != hashlib.sha1(s.encode('utf-8')).hexdigest():
			logging.info('invalid sha1')
			return None
		user.passwd = '******'
		return user
	except Exception as e:
		logging.exception(e)
		return None


# 访问首页
@get('/')
def index(*, page='1'):
	page_index = get_page_index(page)
	num = yield from Blog.findNumber('count(id)')
	page = Page(num)
	if num == 0:
		blogs = []
	else:
		blogs = yield from Blog.findAll(orderBy='created_at desc', limit=(page.offset, page.limit))
	return {
		'__template__': 'blogs.html',
		'page': page,
		'blogs': blogs
		}


# 访问日志页
@get('/blog/{id}')
def get_blog(id):
    blog = yield from Blog.find(id)
    comments = yield from Comment.findAll('blog_id=?', [id], orderBy='created_at desc')
    for c in comments:
        c.html_conetnt = text2html(c.content)
    blog.html_conetnt = markdown2.markdown(blog.content)
    return {
        '__template__': 'blog.html',
        'blog': blog,
        'comments': comments
    }


# 注册页
@get('/register')
def register():
	return {
		'__template__': 'register.html'
	}


# 登录页
@get('/signin')
def signin():
	return {
		'__template__': 'signin.html'
	}


# 验证登陆用户身份后 设置cookie
@post('/api/authenticate') 
def authenticate(*, email, passwd):
	if not email:
		raise APIValueError('email', 'Invalid email.')
	if not passwd:
		raise APIValueError('passwd', 'Invalid password.')
	users = yield from User.findAll('email=?', [email])
	if len(users) == 0:
		raise APIValueError('email', 'Email not exist.')
	user = users[0]
	# check passwd: 存储的sha1是id和密码一起加密后得到的数字串
	sha1 =hashlib.sha1()
	sha1.update(user.id.encode('utf-8'))
	sha1.update(b':')
	sha1.update(passwd.encode('utf-8'))
	if user.passwd != sha1.hexdigest():
		raise APIValueError('passwd', 'Invalid password.')
	# authenticate ok, set cookie:
	r = web.Response()
	r.set_cookie(COOKIE_NAME, user2cookie(user, 86400), max_age=86400, httponly=True)
	user.passwd = '******'
	r.content_type = 'application/json'
	r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
	return r


# 登出页
@get('/signout')
def signout(request):
	referer = request.headers.get('Referer')
	r = web.HTTPFound(referer or '/')
	r.set_cookie(COOKIE_NAME, '-deleted-', max_age=0, httponly=True)
	logging.info('user signed out.')
	return r 


# 管理博客页
@get('/manage/blogs')
def manage_blogs(*, page='1'):
	return {
		'__template__': 'manage_blogs.html',
		'page_index': get_page_index(page)
	}

# 日志创建页
@get('/manage/blogs/create')
def manage_create_blog():
    return {
        '__template__': 'manage_blog_edit.html',
        'id': '', # why id is empty?
        'action': '/api/blogs'
    }


_RE_EMALI = re.compile(r'^[a-z0-9\.\-\_]+\@[a-z0-9\-\_]+(\.[a-z0-9\-\_]+){1,4}$')
_RE_SHA1 = re.compile(r'^[0-9a-f]{40}$')


# 注册用户后即设置cookie
@post('/api/users')  
def api_register_user(*, email, name, passwd):
	if not name or not name.strip():
		raise APIValueError('name')
	if not email or not _RE_EMALI.match(email):
		raise APIValueError('email')
	if not passwd or not _RE_SHA1.match(passwd):
		raise APIValueError('passwd')
	user_1 = yield from User.findAll('email=?', [email])
	if len(user_1) > 0:
		raise APIError('register:failed', 'email', 'Email already used.')
	user_2 = yield from User.findAll('name=?', [name])
	if len(user_2) > 0:
		raise APIError('register:failed', 'name', 'Name already used.')

	uid = next_id()
	sha1_passwd = '%s:%s' % (uid, passwd)
	user = User(id=uid, name=name.strip(), email=email, passwd=hashlib.sha1(sha1_passwd.encode('utf-8')).hexdigest(), image='http://www.gravatar.com/avatar/%s?d=mm&s=120'  % hashlib.md5(email.encode('utf-8')).hexdigest())
	yield from user.save()

	# make session cookie:
	r = web.Response()
	r.set_cookie(COOKIE_NAME, user2cookie(user, 86400), max_age=86400, httponly=True)
	user.passwd = '******'
	r.content_type = 'application/json'
	r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
	return r

# 显示博客页
@get('/api/blogs')
def api_blogs(*, page='1'):
	page_index = get_page_index(page)
	num = yield from Blog.findNumber('count(id)')
	p = Page(num, page_index)
	if num == 0:
		return dict(page=p, blogs=())
	blogs = yield from Blog.findAll(orderBy='created_at desc', limit=(p.offset, p.limit))
	return dict(page=p, blogs=blogs)


# 创建新博客后返回数据
@get('/api/blogs/{id}')
def api_get_blog(*, id):
	blog = yield from Blog.find(id)
	return blog


# 检查用户身份和博客内容
@post('/api/blogs')
def api_create_blog(request, *, name, summary, content):
    check_admin(request)
    if not name or not name.strip():
        raise APIValueError('name', 'name cannot be empty.')
    if not summary or not summary.strip():
        raise APIValueError('summary', 'summray cannot be empty.')
    if not content or not content.strip():
        raise APIValueError('content', 'content cannot be empty.')
    blog = Blog(user_id=request.__user__.id, user_name=request.__user__.name, user_image=request.__user__.image, name=name.strip(), summary=summary.strip(), content=content.strip())
    yield from blog.save()
    return blog 

































