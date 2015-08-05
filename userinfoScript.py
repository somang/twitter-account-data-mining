import sys, json, urllib2, httplib, commands
import MySQLdb
import xlrd
import unicodedata
import time
import datetime
# to collect protected accounts for later use.
check_list = []

def main():
    print 'starting.......'
    # First import the excel file, and make a list of the userid.
    wb = xlrd.open_workbook('/home/namsoman/Somang/accounts_full_oct25.xls')
    sh = wb.sheet_by_index(0)
    userid_lists = sh.col_values(22)
    unique_id = sh.col_values(21)

    try:
	#try_again = ['BeraForCongress','WhipHoyer','SeanBielat','RonPaul']
	#try_cand = [171071021,152051011,103042011,449142012]

        for i in range(48, 600):	#len(userid_lists)):
            if len(userid_lists[i]) > 0: #if the cell is not empty.
                namevar = userid_lists[i]# set userid to get info from
		candidate_id = int(unique_id[i])# this is the name from excel
		
		#namevar = try_again[i]
		#candidate_id = try_cand[i]

                data = basic_info(namevar)# First call

		if data == "queued":
		    protection_bool = True
		else:
		    try:
			    # pick up the categories we need
			    fol_c = data["followers_count"]
			    fri_c = data["friends_count"]
			    protection_bool = data["protected"]
			    id_str = data["id_str"]
	                    print "Name : " + namevar + "/" + str(candidate_id)
			    print "Followers Count : " + str(fol_c)
			    print "Followings Count : " + str(fri_c)

        	    except UnicodeEncodeError:
                	    namevar = unicode(namevar).encode("utf-8")
			    candidate_id = unicode(candidate_id).encode("utf-8")
        	            print "Name : " + namevar + "/" + candidate_id
                	    continue
	            except UnicodeDecodeError:
        	            namevar = unicode(namevar, errors='ignore')
			    candidate_id = unicode(candidate_id, errors='ignore')
	                    print "Name : " + namevar + "/" + candidate_id
        	            continue
		    except:
			protection_bool = True
			check_list.append(namevar)
			continue

                # if the account is protected, then we skip that user.
                if protection_bool == False:
			try:
				# Now we request and add the given data into mysql
				request_followers(namevar, candidate_id, id_str, fol_c)
				request_followings(namevar, candidate_id, id_str, fri_c)
			except:
				check_list.append(namevar)
				continue	
			#find_intersection(cursor, namevar, fol_c, fri_c)
			#conn.commit ()
                elif protection_bool == True:
                    check_list.append(("protected", namevar))
                    print "This account is protected."

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

def conect_mysql_server():#host="localhost", user="root", PhuqAMu2 , tw_politic
    return MySQLdb.connect (host = "localhost", user="namsoman", passwd="PhuqAMu2", db = "tw_politic")

def basic_info(namevar):
    # We first try to Request basic information of given user.
    # set up the request for given user.
    attempt = 0    
    while attempt < 50:
        attempt += 1
        try:
	    cmd = 'twurl "/1.1/users/show.json?screen_name=' + namevar + '"'
	    (status, output) = commands.getstatusoutput(cmd)
	    if status:
		sys.stderr.write(output)
		sys.exit(1)
	    data=json.loads(output)
            print "profile responded"
            break
        except:
	    time.sleep(5.3)
	    check_list.append(namevar)
	    continue
    else:
        data = "queued"
    return data

def fol_req(followers_request):
	attempt = 0
	data = {}
	
	while (attempt < 5 and not(data.get('ids'))):
		try:
			attempt += 1
			(status, output) = commands.getstatusoutput(followers_request)
			if status:
				sys.stderr.write(output)
				sys.exit(1)
			data = json.loads(output)
		except:
			print " error in follower getter. "
			time.sleep(31)
			continue
		print "got response for followers"
		time.sleep(31)
		break
	else:
		print "Error on fol_req"
		check_list.append(followers_request)
	return data

def fir_req(followings_request):
	attempt = 0
	data = {}
	
	while (attempt < 5 and not(data.get('ids'))):
		try:
			attempt += 1
			(status, output) = commands.getstatusoutput(followings_request)
			if status:
				sys.stderr.write(output)
				sys.exit(1)
			data = json.loads(output)
		except:
			print " error in followings getter. "
			time.sleep(31)
			continue
		print "got response for followings"
		time.sleep(31)
		break
	else:
		print "Error on fir_req"
		check_list.append(followings_request)
	return data

def request_followers(namevar, candidate_id, id_str, fol_c):
    # First, get the previous day's number of followers from database.
    conn = conect_mysql_server()
    cur = conn.cursor()
    sql = 'select followers from candidatestats where id_str=' + id_str + ' and date ="' + yesterday() + '"'
    cur.execute(sql)
    prev_fol= int(cur.fetchone()[0])
    cur.close()
    conn.close()

    # Find the difference between the day before, 
    # and see how many pages (multiple of 5000)
    updated_fol = abs(prev_fol - fol_c)
    estimated_call = updated_fol/5000 + 1
    print "new followers :", prev_fol-fol_c, updated_fol, " estimated: ", estimated_call
    next_cursor = -1
    cmd = 'twurl "/1.1/followers/ids.json?cursor=-1&screen_name=' + namevar + '"'
    data = fol_req(cmd)
    
    conn = conect_mysql_server()
    cur = conn.cursor()
    add_followers(cur, data.get('ids'), candidate_id, namevar, id_str)
    conn.commit()
    cur.close()
    conn.close()
    # If there are more new followers than 5000, we go to another page(the one before)
    pager = 1
    try:
	if (data.get('ids') and estimated_call > 1):
		while ((len(data.get('ids')) == 5000) and pager < estimated_call):
			pager+=1
			next_cursor = data['next_cursor']
			followers_request = 'twurl "/1.1/followers/ids.json?cursor=' + str(next_cursor) + '&screen_name=' + namevar+'"'
			try:
				data = fol_req(followers_request)
				conn = conect_mysql_server()
		        	cursor = conn.cursor()
				add_followers(cursor, data.get('ids'), candidate_id, namevar, id_str)
				conn.commit()
				conn.close()
			except:
				continue
    except:
	print "error on", followers_request
	check_list.append(followers_request)

def request_followings(namevar, candidate_id, id_str, fri_c):
    # First, get the previous day's number of followers from database.
    conn = conect_mysql_server()
    cur = conn.cursor()
    sql = 'select followings from candidatestats where id_str=' + id_str + ' and date ="' + yesterday() + '"'
    cur.execute(sql)
    prev_fol= int(cur.fetchone()[0])
    # Find the difference between the day before, 
    # and see how many pages (multiple of 5000)
    updated_fol = abs(prev_fol - fri_c)
    estimated_call = updated_fol/5000 + 1    
    print "new followings :", prev_fol-fri_c, updated_fol," estimated: ", estimated_call
 
    next_cursor = -1
    cmd = 'twurl "/1.1/friends/ids.json?cursor=-1&screen_name=' + namevar + '"'
    data = fir_req(cmd)

    conn = conect_mysql_server()
    cur = conn.cursor()
    add_followings(cur, data.get('ids'), candidate_id, namevar, id_str)
    conn.commit()
    cur.close()
    conn.close()

    pager = 1
    try:
	if (data.get('ids') and estimated_call > 1):
		while ((len(data.get('ids')) == 5000) and pager < estimated_call):
			next_cursor = data['next_cursor']
			pager+=1
			followings_request = 'twurl "/1.1/friends/ids.json?cursor=' + str(next_cursor) + '&screen_name=' + namevar+'"'
			try:
				data = fir_req(followings_request)
				conn = conect_mysql_server()
		        	cursor = conn.cursor()
				add_followings(cursor, data.get('ids'), candidate_id, namevar, id_str)
				conn.commit()
				conn.close()
			except:
				time.sleep(60)
				continue
    except:
	print "error on", followings_request
	check_list.append(followings_request)
	
def add_followers(cursor, followers_list, candidate_id, namevar, id_str):
    # First check if the data already in database.
    indexnumber = 0
    for j in followers_list:
	insert_followers_data(cursor, indexnumber, j, candidate_id, namevar, id_str)
	indexnumber += 1
    print str(indexnumber) + " followers added."
	
def insert_followers_data(cursor, index, item, candidate_id, namevar, id_str):
    # insert into followers table
    cursor.execute ("""
    INSERT INTO followers_2 (id, candidateid, userid, id_str, followersid, date)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (index, candidate_id, namevar, id_str, item, str(today())))
   
def add_followings(cursor, followings_list, candidate_id, namevar, id_str):
    # First check if the data already in database.
    indexnumber = 0
    for f in followings_list:
	insert_followings_data(cursor, indexnumber, f, candidate_id, namevar, id_str)
	indexnumber += 1
    print str(indexnumber) + " followings added."
	
def insert_followings_data(cursor, index, item, candidate_id, namevar, id_str):
    # insert into followings table
    cursor.execute ("""
    INSERT INTO followings_2 (id, candidateid, userid, id_str, followingsid, date)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (index, candidate_id, namevar, id_str, item, str(today())))
      

def find_intersection(cursor, namevar, fol_count, fri_count):
    print "finding the number of intersecting user between followers and followings."
    cursor.execute("""select newt.userid, count(*) friends_count from 
		(select * from followers where exists 
			(select * from followings where 
				followers.followersid = followings.followingsid
                                and
                                followings.id_str = followers.id_str
                                and
                                followings.userid = (%s)
			)) as newt group by userid having friends_count > 0
		""", namevar)
    print "done finding"
    number = cursor.fetchone()
    if number != None:
	number = number[1]
	print number
    else:
        number = 0
    #Check if there is the entry already.
    cursor.execute("""
    SELECT count(*) FROM jt_friends
    WHERE userid = (%s)""", namevar)
    exist = cursor.fetchone()[0]
    if exist == 0: # insert if there isn't the entry yet.
	cursor.execute("""
	INSERT INTO jt_friends 
	(userid, followers_count, followings_count, friends_count) 
	VALUES (%s, %s, %s, %s)
	""", (namevar, fol_count, fri_count, number))
	print "done inserting to jt_friends"
    else: # update if there is the entry.	
	cursor.execute("""
	UPDATE jt_friends
	SET followers_count = %s, followings_count = %s, friends_count = %s
	WHERE userid = %s
	""",(fol_count, fri_count, number, namevar))
	print "done updating to jt_friends"



# Create a daily error log file
def createDailyLog():
	filename = "error-log-" + today()
	f = open(filename, "w")
	f.close()

# Add information to a daily error log file
def appendDailyLog(info):
	filename = "error-log-" + today()
	logfile = open(filename, "w")
	time_now = str(datetime.datetime.now()) + "\n"

	logfile.write(time_now)
	logfile.write(info)

	logfile.close()

def yesterday():
	yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
	return str(yesterday.year) + '-' + str(yesterday.month) + '-' + str(yesterday.day)

def today():
	today = datetime.datetime.today()
	return str(today.year) + '-' + str(today.month) + '-' + str(today.day)

if __name__ == "__main__":
    main()
    print "check list : "
    print check_list
    try: 
	createDailyLog()
	appendDailyLog(str(check_list))
    except:
	print "Error"
    print 'Completed'    
