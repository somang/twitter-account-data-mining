#!/usr/bin/python

# import modules used here
import sys
import commands
import json
import datetime
import time
import MySQLdb as mdb

errorlist = []

'''Pagination - For huge users (tweets above 100),
the results are paginated'''
def tweets(screen_name):
    base_cmd = 'twurl "/1.1/search/tweets.json?q='
    cmd = base_cmd + '%40' + screen_name
    cmd = cmd      + '+since%3A' + yesterday()
    cmd = cmd      + '+until%3A' + today()
    cmd = cmd      + '&count=100"'
    dict = {}
    
    tweets_list = []
    print screen_name
    
    try:
        waitHits(30)
            
        (status, output) = commands.getstatusoutput(cmd)
        if status:
            print "error on status"
            sys.stderr.write(output)
            sys.exit(1)
        
        dict = json.loads(output)
            
        statuses = dict["statuses"]
        search_metadata = dict["search_metadata"]
        
        if ("next_results" in search_metadata):
            print "paginated"
            tweets_list.extend(statuses)
            tweets_list = pagination(screen_name, search_metadata["next_results"], tweets_list)
        else:
            tweets_list.extend(statuses)
    except:
        if not(names[i] in errorlist):
            errorlist.append(names[i])
        print "error", screen_name, "so far:", errorlist
    
                
    print "Screen Name:", screen_name , "Number of Mentions:", len(tweets_list), "Date:", yesterday()
    return tweets_list


def pagination(screen_name,next_results, tweets_list):
    cmd = 'twurl "/1.1/search/tweets.json' + next_results + '"'
    dict = {}
    
    while not(("statuses" in dict) and ("search_metadata" in dict)):
        waitHits(30)

        try:
            (status, output) = commands.getstatusoutput(cmd)
            if status:
                sys.stderr.write(output)
                sys.exit(1)
            dict = json.loads(output)
            statuses = dict["statuses"]
            search_metadata = dict["search_metadata"]
	    if ("next_results" in search_metadata):
		print "paginated"
        	tweets_list.extend(statuses)
       		tweets_list = pagination(screen_name, search_metadata["next_results"], tweets_list)
	    else:
       		tweets_list.extend(statuses)
        except:
            continue
    return tweets_list

        
#    INSERT INTO dateplayground (dp_name, dp_timestamp)
#	    VALUES ('TIMESTAMP: Manual Timestamp', '1776-7-4 04:13:54')";
def date_converter(date_ts):
    YYYY = date_ts[-4:] # 2012
    MM = date_ts[4:7] # Oct
    DD = date_ts[8:10] # 4
    TT = date_ts[11:19] # 04:13:54
    
    monthmap ={'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    
    return YYYY + '-' + monthmap[MM] + '-' + DD + ' ' + TT


def get_sql(userid, names, table, statuses, cred, num_mentions):
    
    con = mdb.connect(cred['host'], cred['user'], cred['passwd'] , cred['dbase'])
    
    if table == 'mentions':
        rt_user = statuses["user"]
        date = date_converter(statuses["created_at"])

        with con:
            cur = con.cursor()
            try:
                cur.execute ("""
            INSERT INTO mentions (can_id_str, can_userid, id_str, text, source, retweet_count, retweeted, by_whom, favorited, irtu_id, irts_id, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (str(userid), names, statuses["id_str"], statuses["text"], statuses["source"], str(statuses["retweet_count"]), str(statuses["retweeted"]), rt_user["id_str"], str(statuses["favorited"]), str(statuses["in_reply_to_user_id_str"]), str(statuses["in_reply_to_status_id_str"]), date))
            except UnicodeEncodeError:
                n_names = unicode(names).encode("utf-8")
                n_text = unicode(statuses["text"]).encode("utf-8")
                n_source = unicode(statuses["source"]).encode("utf-8")
                cur.execute ("""
                        INSERT INTO mentions (can_id_str, can_userid, id_str, text, source, retweet_count, retweeted, by_whom, favorited, irtu_id, irts_id, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (userid, n_names, statuses["id_str"], n_text, n_source, str(statuses["retweet_count"]), str(statuses["retweeted"]), rt_user["id_str"], str(statuses["favorited"]), str(statuses["in_reply_to_user_id_str"]), str(statuses["in_reply_to_status_id_str"]), date))
            con.commit()
        con.close()
            
    elif table == 'mentionuser':
        rt_user = statuses["user"]
        usr_date = date_converter(rt_user["created_at"])

        with con:
            cur = con.cursor()
            try:
                cur.execute ("""
                    INSERT INTO mentionuser (id_str, description, location, name, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """, (str(rt_user["id_str"]), rt_user["description"], rt_user["location"], rt_user["name"], usr_date))
            except UnicodeEncodeError:
                n_description = unicode(rt_user["description"]).encode("utf-8")
                n_location = unicode(rt_user["location"]).encode("utf-8")
                n_user = unicode(rt_user["name"]).encode("utf-8")
                cur.execute ("""
                    INSERT INTO mentionuser (id_str, description, location, name, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """, (rt_user["id_str"], n_description, n_location, n_user, usr_date))
            con.commit()
        con.close()
            
    elif table == 'mentionstats':
        with con:
            cur = con.cursor()
            try:
                cur.execute ("""
                    INSERT INTO mentionstats (id_str, num_mentions, date)
                    VALUES (%s, %s, %s)
                    """, (userid, num_mentions, yesterday()))
            except UnicodeEncodeError:
                n_userid = unicode(userid).encode("utf-8")
                cur.execute ("""
                    INSERT INTO mentionstats (id_str, num_mentions, date)
                    VALUES (%s, %s, %s)
                    """, (userid, num_mentions, yesterday()))
            con.commit()
        con.close()


''' Checks that the remaining number of API requests is always above a safe buffer and
    ensures that the API limit is never reached.
'''
# waits for six seconds before another call to the twitter API version 1.1. If the rate is less than the buffer rate make it sleep for 15 minutes
def waitHits(bufferRate):
    rate = ratechecker()

    if rate > bufferRate:
        time.sleep(6)
    else:
        while (rate < bufferRate):
            print "Waiting - Rate is low ", rate
            time.sleep(60) #check every minute
            rate = ratechecker()

def ratechecker():
    cmd = 'twurl "/1.1/application/rate_limit_status.json?resources=search"'        
    (status,output) = commands.getstatusoutput(cmd)
    if status:
        sys.strderr.write(output)
        sys.exit(1)
    dict = json.loads(output)
    rate = dict["resources"]["search"]["/search/tweets"]["remaining"]
    return rate

''' Returns a string version of the date today.
    @return todays date - format YYYY-MM-DD.
    '''
def today():
    today = datetime.datetime.today()
    return str(today.year) + '-' + str(today.month) + '-' + str(today.day)
    #return '2012-12-27'

#   Returns a string version of the date yesterday.
def yesterday():
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    return str(yesterday.year) + '-' + str(yesterday.month) + '-' + str(yesterday.day)
    #return '2012-12-26'    

''' Reads a tweeter screen names from a specified flat file.
    @param filename - the flat file containing tweeter screennames.
    @return a list of screen names.
'''
def loadTwitterFile(filename):
    twitter = []
    f = open(filename, "rU")
    for line in f: 
        twitter.append(line.strip("\n"))
    f.close()
    return twitter

# Saves twitter information into our database
def saveSQL(cred, sql):
    con = mdb.connect(cred['host'], cred['user'], cred['passwd'] , cred['dbase'])
    with con:
        cur = con.cursor()
        for statement in sql:
            cur.execute (statement)
    con.close()

# Saves twitter information into our database
def loadScreenName(cred, table):
        ids = []
        names = []    
        con = mdb.connect(cred['host'], cred['user'], cred['passwd'] , cred['dbase'])
        sql = 'select screen_name, id_str from ' + table
        with con:
		cur = con.cursor(mdb.cursors.DictCursor)
		cur.execute (sql)
		rows = cur.fetchall()
                for row in rows:
                    ids.append(row['id_str'])
                    names.append(row['screen_name'])
	con.close()
	return ids, names

# Saves twitter information into our database
def loadID(cred, errorlist):
    ids = []
    names = []
    screennames = ""
    screennames = screennames + '"' + errorlist[0] + '"'
    for i in errorlist[1:]:
        screennames = screennames + ' or screen_name = "' + i + '"'
    con = mdb.connect(cred['host'], cred['user'], cred['passwd'] , cred['dbase'])
    sql = 'select screen_name, id_str from candidates where screen_name = ' + screennames
    with con:
            cur = con.cursor(mdb.cursors.DictCursor)
            cur.execute (sql)
            rows = cur.fetchall()
            for row in rows:
                ids.append(row['id_str'])
                names.append(row['screen_name'])
    con.close()
    return ids, names

# Inserts new table to candidates and media table - unique ids.
def insertNewAccount(cred, filename, table):
    for handle in loadTwitterFile(filename):
        if handle not in loadScreenName(cred, table):
            print 'add insert SQL for', handle

def main():
    cred =  {'host':'localhost', 'user':'namsoman', 'passwd':'PhuqAMu2', 'dbase':'tw_politic'}
    ids, names = loadScreenName(cred,"candidates")
    trylist = ['BarackObama', 'MittRomney']
    for i in range(0,len(ids)):
        if (names[i] in trylist):
            print names[i]
        else:
            try:
                tweets_list = tweets(names[i])
                if not(names[i] in errorlist):
                    for statuses in tweets_list:
                        get_sql(ids[i], names[i], "mentions", statuses, cred, len(tweets_list))
                        get_sql(ids[i], names[i], "mentionuser", statuses, cred, len(tweets_list))
                    get_sql(ids[i], names[i], "mentionstats", tweets_list, cred, len(tweets_list))
            except:
                if not(names[i] in errorlist):
                    errorlist.append(names[i])
                continue

def retry():
    cred =  {'host':'localhost', 'user':'namsoman', 'passwd':'PhuqAMu2', 'dbase':'tw_politic'}

    trylist = ['elizabethforma', 'RickNolan2012', 'RepJoseSerrano', 'joaquincastrotx', 'MiaBLove']

    ids,names = loadID(cred, trylist)
    for i in range(0, len(trylist)):
        try:
            tweets_list = tweets(names[i])
            if not(names[i] in errorlist):
                for statuses in tweets_list:
                    get_sql(ids[i], names[i], "mentions", statuses, cred, len(tweets_list))
                    get_sql(ids[i], names[i], "mentionuser", statuses, cred, len(tweets_list))
                get_sql(ids[i], names[i], "mentionstats", tweets_list, cred, len(tweets_list))
        except:
            if not(names[i] in errorlist):
                errorlist.append(names[i])
            continue

# Standard boilerplate to call the main function to begin the program
if __name__ == '__main__':
    main()
    #retry()
    time2 = str(datetime.datetime.now())
    print time2
    print errorlist
