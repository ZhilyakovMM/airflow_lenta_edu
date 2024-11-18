from impala.dbapi import connect

conn = connect(
    '<host>',
    21050,
    auth_mechanism="LDAP",
    user='<login>',
    password='<password>'
)
cur = conn.cursor()
cur.execute('SHOW DATABASES;')
result = cur.fetchall()
for data in result:
    print(data)
