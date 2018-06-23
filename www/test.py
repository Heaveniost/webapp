# -*- coding: utf-8 -*-

import sys
import orm, asyncio
from models import User, Blog, Comment

def test(loop):
	yield from orm.create_pool(loop=loop, user='root', password='newpass', db='awesome')

	u = User(name='Test', email='test@163.com', passwd='12345', image='about:blank')

	yield from u.save()

if __name__ == '__main__':

	loop = asyncio.get_event_loop()
	loop.run_until_complete(asyncio.wait([test(loop)]))
	loop.close()
	if loop.is_closed():
		sys.exit(0)