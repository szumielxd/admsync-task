import mysql.connector
from configparser import ConfigParser
import asyncio
import time


async def fetchModeratorsByGroup(mydb) -> dict[str, set[str]]:
	"""
	Fetch current groups and its members from adminmanager database.

	Parameters:
	mydb (connection): mysql connection to adminmanager database
	
	Returns:
	dict[str, set[str]]: dictionary where key is group name and value is list of its member uuids
	"""

	with mydb.cursor() as cr:
		query = """
			SELECT `uuid`, `g`.`minecraft_id` `group_name`
				FROM `users` `u`
				RIGHT JOIN `groups` `g` ON `u`.`group_id` = `g`.`id`
				WHERE `u`.`id` IS NULL OR (`frozen` = false AND `g`.`minecraft_id` IS NOT NULL)
				ORDER BY `g`.`weight` DESC
			"""
		cr.execute(query)
		res = cr.fetchall()
		cr.close()
		groups = {}
		for (uuid, groupName) in res:
			if groupName not in groups:
				groups[groupName] = set()
			if uuid is not None:
				groups[groupName].add(uuid)
		return groups


async def fetchCurrentModeratorsByGroup(mydb, groups: set[str]) -> dict[str, set[str]]:
	"""
	Fetch current selected groups and its members from luckperms database.

	Parameters:
	mydb (connection): mysql connection to luckperms database
	groups (set[str]): list of groups to fetch
	
	Returns:
	dict[str, set[str]]: dictionary where key is group name and value is list of its member uuids
	"""

	with mydb.cursor() as cr:
		query = '''
			SELECT `uuid`, `permission`
				FROM `luckperms_user_permissions`
				WHERE `permission` IN ({groups}) AND `value` = 1 AND `contexts` = '{{}}'
			'''.format(groups = (', %s' * len(groups))[2:] )
		params = [ 'group.' + x for x in groups ]
		cr.execute(query, params = params)
		res = cr.fetchall()
		cr.close()
		current_groups = { k: set() for k in groups }
		for (uuid, permission) in res:
			group = permission[len('group.'):]
			current_groups[group].add(uuid)
		return current_groups


async def logUsersAction(cr, uuids: set[str], action: str):
	"""
	Logs user modification action to luckperms database.

	Parameters:
	cr (cursor): mysql connection cursor inside luckperms database
	uuids (set[str]): uuids of users to log action for
	action (str): action to log
	"""

	cr.executemany('''INSERT INTO `luckperms_actions` (`time`, `actor_uuid`, `actor_name`, `type`, `acted_uuid`, `acted_name`, `action`)
			VALUES (%s, '00000000-0000-0000-0000-000000000000', 'AdmSync@bot', 'U', %s, COALESCE((SELECT `username` FROM `luckperms_players` WHERE `uuid` = %s), 'null'), %s)''',
			[ (int(time.time()), uuid, uuid, action ) for uuid in uuids ])


async def removeMembers(cr, group: str, members: set[str]):
	"""
	Remove users from specific group.

	Parameters:
	cr (cursor): mysql connection cursor inside luckperms database
	group (str): group's name
	members (set[str]): list of user's uuids to remove
	"""

	await logUsersAction(cr, members, 'parent remove ' + group)
	query_delete = '''
		DELETE FROM `luckperms_user_permissions`
			WHERE `permission` = %s AND `uuid` IN ({uuids}) AND `value` = 1 AND `contexts` = '{{}}'
		'''.format(uuids = (', %s' * len(members))[2:] )
	cr.execute(query_delete, params = [ 'group.' + group, *members ])
	return len(members)


async def addMembers(cr, group: str, members: set[str]):
	"""
	Add users to specific group.

	Parameters:
	cr (cursor): mysql connection cursor inside luckperms database
	group (str): group's name
	members (set[str]): list of user's uuids to add
	"""

	await logUsersAction(cr, members, 'parent add ' + group)
	query_add = '''
		INSERT INTO `luckperms_user_permissions` (`uuid`, `permission`, `value`, `server`, `world`, `expiry`, `contexts`)
			VALUES (%s, %s, '1', 'global', 'global', '0', '{}')
		'''
	params = [ ( uuid, 'group.' + group ) for uuid in members ]
	cr.executemany(query_add, params)
	return len(members)


async def updateGroups(mydb, current_groups: dict[str, set[str]], groups: dict[str, set[str]]):
	"""
	Update users inside groups to match expected state. 

	Parameters:
	mydb (connection): mysql connection to luckperms database
	current_groups (dict[str, set[str]]): dictionary of current groups and its members where key is group name and value is list of user's uuids
	groups (dict[str, set[str]]): dictionary of expected groups and its members where key is group name and value is list of user's uuids
	"""

	toRemove = { k: current_groups[k] - groups[k] for k in groups }
	toAdd = { k: groups[k] - current_groups[k] for k in groups }
	added = 0
	removed = 0
	with mydb.cursor() as cr:
		for (group, members) in toRemove.items():
			if len(members):
				removed += await removeMembers(cr, group, members)
		for (group, members) in toAdd.items():
			if len(members):
				added += await addMembers(cr, group, members)
	if added or removed:
		mydb.commit()
		print("[AdmSync] Successfully added {} and removed {} user-entries".format(added, removed))


async def syncTask():
	"""
	Update users inside luckperms groups to match state of adminmanager database.
	"""

	config = ConfigParser()
	config.read('admsync.ini')
	config_adm = {
		"host": config.get('adminmanager_db', 'db_host'),
		"database": config.get('adminmanager_db', 'db_name'),
		"user": config.get('adminmanager_db', 'db_user'),
		"password": config.get('adminmanager_db', 'db_password'),
		"collation": "utf8mb4_unicode_ci"
	}
	config_lp = {
		"host": config.get('luckperms_db', 'db_host'),
		"database": config.get('luckperms_db', 'db_name'),
		"user": config.get('luckperms_db', 'db_user'),
		"password": config.get('luckperms_db', 'db_password'),
		"collation": "utf8mb4_unicode_ci"
	}

	with mysql.connector.connect(**config_adm) as admdb:
		with mysql.connector.connect(**config_lp) as lpdb:
			groups = await fetchModeratorsByGroup(admdb)
			current_groups = await fetchCurrentModeratorsByGroup(lpdb, groups.keys())
			await updateGroups(lpdb, current_groups, groups)

asyncio.run(syncTask())