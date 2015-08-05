#!/usr/bin/python

# import modules used here
import sys
import commands
import json
import datetime
import time
import MySQLdb as mdb

## Returns todays date in the format YYYY-MM-DD
def today():
	today = datetime.datetime.today()
	return str(today.year) + '-' + str(today.month) + '-' + str(today.day)
	#return '2012-12-30'

## Converts the twitter date in the format YYYY-MM-DD
def createdAt(timestamp):
	DD = timestamp.split()[2]
	MM = timestamp.split()[1]
	YYYY = timestamp.split()[5]

	monthmap ={'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

	return YYYY + '-' + monthmap[MM] + '-' + DD

## Returns the remaining number of hits left in the API rate
def remainingHits():
	cmd = 'twurl /1/account/rate_limit_status.json'
	
	(status, output) = commands.getstatusoutput(cmd)
	if status:
		sys.stderr.write(output)
		sys.exit(1)

	dict = json.loads(output)
	return dict['remaining_hits']

## Checks the remaining API hits and waits until the next refresh
## if the remaining API hits are less than 50.
def checkRate():
	rate = remainingHits()
	print 'Running - Rate is high at : ', rate
	while rate < 10:
		print "Waiting - Rate is low at : ", rate
		time.sleep(60 * 1)
		rate = remainingHits()

#Loads Initial Twitter accounts to the program from a text file
def loadFile(filename):
		twitter = []
		f = open(filename, "r")
		# OR read all the lines into a list.
		lines = f.readlines()
		f.close()
		return lines


## Returns a list of Sql statements needed to insert or update the database
## with twitter information.
def getSQL(ident, table, userid):
	cmd = 'twurl ' 
	if (userid):
		cmd = cmd + '/1.1/users/show.json?user_id=' + ident
	else:
		cmd = cmd + '/1.1/users/show.json?screen_name=' + ident

	(status, output) = commands.getstatusoutput(cmd)
	if status:    ## Error case, print the command's output to stderr and exit
		sys.stderr.write(output)
		sys.exit(1)

	dicta = json.loads(output)


	## Create SQL Statements
	sql = ''
	if (table == 'mediastats' or table == 'candidatestats'):
		sql = 'insert into ' + table + ' (id_str, followers, followings, lists, tweets, date)'
		sql = sql + ' values (\'' + dicta['id_str'] + '\', \'' + str(dicta['followers_count']) + '\', \'' + str(dicta['friends_count']) 
		sql = sql + '\', \'' + str(dicta['listed_count']) +'\', \'' + str(dicta['statuses_count']) + '\', \''+ today() + '\')'

	elif (table == 'media' or table == 'candidates'):
		sql = 'insert into ' + table + ' (id_str, name, description, screen_name, verified, created_at, location)'
		sql = sql + ' values (\'' + dicta['id_str'] + '\', \'' + '' + '\', \'' + '' + '\', \'' + dicta['screen_name']
		sql = sql + '\', \'' + '' + '\', \'' + createdAt(dicta['created_at']) + '\', \'' + '' + '\')'

	return sql

# Saves twitter information into our database
def saveSQL(sql):
	host = 'localhost'
	user = 'namsoman'
	passwd = 'PhuqAMu2'
	dbase = 'tw_politic'

	con = mdb.connect(host, user, passwd,dbase)

	with con:
		cur = con.cursor()
		cur.execute (sql)
	
	con.close()

# Saves twitter information into our database
def loadIdStr(table):
        host = 'localhost'
        user = 'namsoman'
        passwd = 'PhuqAMu2'
        dbase = 'tw_politic'
	ids = []
	con = mdb.connect(host, user, passwd,dbase)
	sql = 'select * from ' + table

	with con:
		cur = con.cursor(mdb.cursors.DictCursor)
		cur.execute (sql)
		rows = cur.fetchall()
		
		for row in rows:
			ids.append(row['id_str'])
	con.close()
	return ids

# Create a daily error log file
def createDailyLog():
	filename = "/home/namsoman/Somang/logs/error-log-" + today()
	f = open(filename, "w")
	f.close()

# Add information to a daily error log file
def appendDailyLog(info):
	filename = "/home/namsoman/Somang/logs/error-log-" + today()
	#logfile = appendDailyLog(filename)
	logfile = open(filename, "w")
	logfile.write(info)
	logfile.close()

errorlist = []

# Gather our code in a main () function
def main():
	createDailyLog()
	twitter = loadIdStr('candidates')
	for line in twitter:
		try:
			#checkRate()
			print line
			time.sleep(5.3)
			saveSQL(getSQL(line,'candidatestats', True))
		except:
			appendDailyLog(line)
			errorlist.append(line)
			continue
	print 'Completed', errorlist

def retry(try_list):
	for line in try_list:
		try:
			#checkRate()
			print line
			time.sleep(5.3)
			saveSQL(getSQL(line,'candidatestats',True))
		except:
			errorlist.append(line)
			continue
	print errorlist
	

# Standard boilerplate to call the main() function to begin the program
if __name__ == '__main__':
	main()
	#retry([])
