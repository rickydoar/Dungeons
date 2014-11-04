import time
import urllib #for url encoding
import base64
import json
import eventlet
from eventlet.green import urllib2
import datetime
import calendar
import uuid
import random
import struct
import socket
from dateutil import relativedelta

def simulate_users(amount, user_file, date):
	registration(amount, date)
	retained(user_file, date)
	stupid_file_switch("registered_users.txt", "new_users.txt")

def track(events):
	data = base64.b64encode(json.dumps(events))
	host = 'api.mixpanel.com'
	params = {
		'data': data,
		'verbose':1,
		'ip':0,
		'api_key':api_key
	}
	url = 'http://%s/%s/' % (host, 'import')
	response = urllib2.urlopen(url, urllib.urlencode(params))
	message = response.read()

	if json.loads(message)['status'] != 1:
	    print message

def event_batcher(eventlist):
	events = []
	pool = eventlet.GreenPool(size=200)
	for event in eventlist:
		events.append(event)
		if len(events) == 50:
			pool.spawn(track, events)
			events = []
	pool.waitall()
	if len(events):
		track(events)
		print "Sent remaining %d events!" % len(events)

def people_update(userlist, operator):
		url = "http://api.mixpanel.com/engage/"
		batch = []
		for user in userlist:
			distinctid = user['properties']['distinct_id']
			tempparams = {
				'token':token,
				'$distinct_id':distinctid,
				"$ignore_time":"True"
				}
			if operator == "$set":
				tempparams.update({'$set':user['people properties']})
				tempparams.update({'$ip':user['properties']['ip']})
			elif operator == "$append":
				tempparams.update({operator:{"$transactions":user["$transactions"]}})
			elif operator == "$add":
				tempparams.update({operator:{"Revenue":user["Revenue"]}})
			batch.append(tempparams)

		payload = {"data":base64.b64encode(json.dumps(batch)), "verbose":1, "ip":0}
		response = urllib2.urlopen(url, urllib.urlencode(payload))
		message = response.read()
		if json.loads(message)['status'] != 1:
			print message

def people_batcher(users, operator):
        pool = eventlet.GreenPool(size=200)
        batch_list = []
        while len(users):
            batch = users[:50]
            batch_list.append(batch)
            users = users[50:]
            x = 0
        for batch in batch_list:
            x+=1
            pool.spawn(people_update, batch, operator)
        pool.waitall()

def build_user():
	user = {"properties":{"distinct_id":str(uuid.uuid4()), "token":token}, "people properties":{}, "registration":1, "registration_retention":1, "retention":1, "conversion":1}
	user["properties"].update({"ip":socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))})
	first_names = ['James','John','Robert','Michael','William','David','Richard','Charles','Joseph','Thomas','Christopher','Daniel','Paul','Mark','Donald','George','Kenneth','Steven','Edward','Brian','Ronald','Anthony','Kevin','Jason','Matthew','Gary','Timothy','Jose','Larry','Jeffrey','Frank','Scott','Eric','Stephen','Andrew','Raymond','Gregory','Joshua','Jerry','Dennis','Walter','Patrick','Peter','Harold','Douglas','Henry','Carl','Arthur','Ryan','Roger','Joe','Juan','Jack','Albert','Jonathan','Justin','Terry','Gerald','Keith','Samuel','Willie','Ralph','Lawrence','Nicholas','Roy','Benjamin','Bruce','Brandon','Adam','Harry','Fred','Wayne','Billy','Steve','Louis','Jeremy','Aaron','Randy','Howard','Eugene','Carlos','Russell','Bobby','Victor','Martin','Ernest','Phillip','Todd','Jesse','Craig','Alan','Shawn','Clarence','Sean','Philip','Chris','Johnny','Earl','Jimmy','Antonio','Danny','Bryan','Tony','Luis','Mike','Stanley','Leonard','Nathan','Dale','Manuel','Rodney','Curtis','Norman','Allen','Marvin','Vincent','Glenn','Jeffery','Travis','Jeff','Chad','Jacob','Lee','Melvin','Alfred','Kyle','Francis','Bradley','Jesus','Herbert','Frederick','Ray','Joel','Edwin','Don','Eddie','Ricky','Troy','Randall','Barry','Alexander','Bernard','Mario','Leroy','Francisco','Marcus','Micheal','Theodore','Mary','Patricia','Linda','Barbara','Elizabeth','Jennifer','Maria','Susan','Margaret','Dorothy','Lisa','Nancy','Karen','Betty','Helen','Sandra','Donna','Carol','Ruth','Sharon','Michelle','Laura','Sarah','Kimberly','Deborah','Jessica','Shirley','Cynthia','Angela','Melissa','Brenda','Amy','Anna','Rebecca','Virginia','Kathleen','Pamela','Martha','Debra','Amanda','Stephanie','Carolyn','Christine','Marie','Janet','Catherine','Frances','Ann','Joyce','Diane','Alice','Julie','Heather','Teresa','Doris','Gloria','Evelyn','Jean','Cheryl','Mildred','Katherine','Joan','Ashley','Judith','Rose','Janice','Kelly','Nicole','Judy','Christina','Kathy','Theresa','Beverly','Denise','Tammy','Irene','Jane','Lori','Rachel','Marilyn','Andrea','Kathryn','Louise','Sara','Anne','Jacqueline','Wanda','Bonnie','Julia','Ruby','Lois','Tina','Phyllis','Norma','Paula','Diana','Annie','Lillian','Emily','Robin','Peggy','Crystal','Gladys','Rita','Dawn','Connie','Florence','Tracy','Edna','Tiffany','Carmen','Rosa','Cindy','Grace','Wendy','Victoria','Edith','Kim','Sherry','Sylvia','Josephine']
	last_names = ['Smith','Johnson','Williams','Jones','Brown','Davis','Miller','Wilson','Moore','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Garcia','Martinez','Robinson','Clark','Rodriguez','Lewis','Lee','Walker','Hall','Allen','Young','Hernandez','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker','Gonzalez','Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans','Edwards','Collins','Stewart','Sanchez','Morris','Rogers','Reed','Cook','Morgan','Bell','Murphy','Bailey','Rivera','Cooper','Richardson','Cox','Howard','Ward','Torres','Peterson','Gray','Ramirez','James','Watson','Brooks','Kelly','Sanders','Price','Bennett','Wood','Barnes','Ross','Henderson','Coleman','Jenkins','Perry','Powell','Long','Patterson','Hughes','Flores','Washington','Butler','Simmons','Foster','Gonzales','Bryant','Alexander','Russell','Griffin','Diaz','Hayes','Myers','Ford','Hamilton','Graham','Sullivan','Wallace','Woods','Cole','West','Jordan','Owens','Reynolds','Fisher','Ellis','Harrison','Gibson','Mcdonald','Cruz','Marshall','Ortiz','Gomez','Murray','Freeman','Wells','Webb','Simpson','Stevens','Tucker','Porter','Hunter','Hicks','Crawford','Henry','Boyd','Mason','Morales','Kennedy','Warren','Dixon','Ramos','Reyes','Burns','Gordon','Shaw','Holmes','Rice','Robertson','Hunt','Black','Daniels','Palmer','Mills','Nichols','Grant','Knight','Ferguson','Rose','Stone','Hawkins','Dunn','Perkins','Hudson','Spencer','Gardner','Stephens','Payne','Pierce','Berry','Matthews','Arnold','Wagner','Willis','Ray','Watkins','Olson','Carroll','Duncan','Snyder','Hart','Cunningham','Bradley','Lane','Andrews','Ruiz','Harper','Fox','Riley','Armstrong','Carpenter','Weaver','Greene','Lawrence','Elliott','Chavez','Sims','Austin','Peters','Kelley','Franklin','Lawson','Fields','Gutierrez','Ryan','Schmidt','Carr','Vasquez','Castillo','Wheeler','Chapman','Oliver','Montgomery','Richards','Williamson','Johnston','Banks','Meyer','Bishop','Mccoy','Howell','Alvarez','Morrison','Hansen','Fernandez','Garza','Harvey','Little','Burton','Stanley','Nguyen','George','Jacobs','Reid','Kim','Fuller','Lynch','Dean','Gilbert','Garrett','Romero','Welch','Larson','Frazier','Burke','Hanson','Day','Mendoza','Moreno','Bowman','Medina','Fowler','Brewer','Hoffman','Carlson','Silva','Pearson','Holland','Douglas','Fleming','Jensen','Vargas','Byrd','Davidson']
	email_domains = ["gmail", "yahoo", "aol", "hotmail"]
	email_words = ['dragon','lancer','sword','fire','magic','dance','random','killer','hacker','pike','trebuchet','catapult','iron','ranger','bow','arrow','strafe','hound','wiggle','darkness','light','coward','hero','giant','troll','dog','wolf','bear','puma','lion','pterodactyl','love','shadow','x']
	marketing_campaign_source = [({"value":"Twitter", "registration":.1, "retention":-.1, "conversion":.05, "name":"Campaign Source"}, 20), ({"value":"Facebook", "registration":.1, "retention":-.1, "conversion":0, "name":"Campaign Source"}, 55), ({"value":"LinkedIn", "registration":-.2, "retention":-.05, "conversion":-.05, "name":"Campaign Source"}, 15),({"value":"Email", "registration":-.2, "retention":-.1, "conversion":-.1, "name":"Campaign Source"}, 12), ({"value":"Organic", "registration":.1, "retention":0, "conversion":0, "name":"Campaign Source"}, 70), ({"value":"Google Adwords", "registration":.1, "retention":-.1, "conversion":.1, "name":"Campaign Source"}, 10)]
	marketing_campaign_name = [({"value":"Super Sale", "registration":.1, "retention":-.1, "conversion":.1, "name":"Campaign Name"}, 20), ({"value":"Buy Now", "registration":.2, "retention":-.2, "conversion":0, "name":"Campaign Name"}, 15), ({"value":"Huge Discounts!", "registration":-.2, "retention":-.1, "conversion":-.1, "name":"Campaign Name"}, 25)]
	operating_system = [({"value":"Android", "registration":0, "retention":0, "conversion":0, "name":"$os"}, 20),({"value":"iPhone OS", "registration":-.1, "retention":-.1, "conversion":-.1, "name":"$os"}, 80)]
	invited_user = [({"value":"True", "registration":.2, "retention":.1, "conversion":.15, "name":"Invited User?"}, 10), ({"value":"False", "registration":0, "retention":0, "conversion":0, "name":"Invited User?"}, 100)]
	app_version = [({"value":"1", "registration":0, "retention":-.2, "conversion":-.2, "name":"App Version"}, 5),({"value":"2", "registration":0, "retention":-.1, "conversion":-.1, "name":"App Version"}, 10), ({"value":"3", "registration":0, "retention":.05, "conversion":.05, "name":"App Version"}, 90)]
	experiment_group = [({"value":"Group A", "registration":.1, "retention":.1, "conversion":.1, "name":"Experiment Group"}, 20), ({"value":"Group B", "registration":-.1, "retention":-.1, "conversion":.1, "name":"Experiment Group"}, 20), (False, 60)]
	super_properties = [invited_user, app_version, experiment_group, operating_system, marketing_campaign_source, marketing_campaign_name]
	user = modify_user(user, super_properties)
	iphone_models = [({"value":"iPhone4,1", "registration":0, "retention":.1, "conversion":0, "name":"$model"},40) , ({"value":"iPhone3,1", "registration":-.1, "retention":0, "conversion":0, "name":"$model"},35), ({"value":"iPhone6,1", "registration":0, "retention":.1, "conversion":0, "name":"$model"},50), ({"value":"iPhone5,2", "registration":0, "retention":0, "conversion":0, "name":"$model"},30), ({"value":"iPhone6,1", "registration":0, "retention":.1, "conversion":.1, "name":"$model"},45), ({"value":"iPhone5,1", "registration":0, "retention":0, "conversion":0, "name":"$model"},28), ({"value":"iPod5,1", "registration":.1, "retention":.1, "conversion":.1, "name":"$model"},22), ({"value":"iPad2,5", "registration":-.1, "retention":0, "conversion":.1, "name":"$model"},20), ({"value":"iPad3,4", "registration":-.1, "retention":.1, "conversion":.1, "name":"$model"},15), ({"value":"iPad4,1", "registration":-.2, "retention":.1, "conversion":0, "name":"$model"},10)]
	android_models = [({"value":"GT-I9300", "registration":-.1, "retention":.1, "conversion":.1, "name":"$model"},45), ({"value":"GT-I9500", "registration":0, "retention":.1, "conversion":0, "name":"$model"},40), ({"value":"SM-G900F", "registration":-.1, "retention":0, "conversion":0, "name":"$model"},35), ({"value":"GT-I8190L", "registration":.1, "retention":.1, "conversion":0, "name":"$model"},32), ({"value":"XT1032", "registration":0, "retention":0, "conversion":0, "name":"$model"},28), ({"value":"Nexus 5", "registration":-.1, "retention":0, "conversion":0, "name":"$model"},25), ({"value":"LG-D802", "registration":.1, "retention":.1, "conversion":.1, "name":"$model"},20)]
	if user["properties"]["$os"] == "iPhone OS":
		modify_user(user, [iphone_models])
	if user["properties"]["$os"] == "Android":
		modify_user(user, [android_models])
	referrers = ['Organic', 'http://bing.com','http://google.com','http://facebook.com','http://twitter.com','http://reddit.com','http://baidu.com','http://duckduckgo.com']
	email = "%s.%s@%s.com" % (random.choice(email_words), random.choice(email_words), random.choice(email_domains))
	referrer = random.choice(referrers)
	user['people properties'].update({"$first_name":random.choice(first_names), "$last_name":random.choice(last_names), "$email":email})
	user["properties"].update({"Referrering Domain":referrer})
	user["people properties"].update({"Referrering Domain":referrer})
	return user

def modify_user(user, super_properties):
	organic = False
	for list_prop in super_properties:
		prop = weighted_choice(list_prop)
		if prop:
			if prop["value"] == "Organic":
				organic = True
			if organic:
				prop["value"] = "Organic"
			user["properties"].update({prop["name"]:prop["value"]})
			user["people properties"].update({prop["name"]:prop["value"]})
			user["retention"] += prop["retention"]
			user["conversion"] += prop["conversion"]
			user["registration"] += prop["registration"]
		if user["retention"] >= 2:
			user["retention"] = 1.8
	return user

def weighted_choice(choices):
	total = sum(w for c, w in choices)
	r = random.uniform(0, total)
	upto = 0
	for c, w in choices:
		if upto + w > r:
			return c
		upto += w


def registration(amount, date):
	events = []
	registered_users = []
	f = open('new_users.txt', 'a')
	for x in range(amount):
		user = build_user()
		registered = False
		timestamp = calendar.timegm(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())
		final_timestamp = timestamp + 86400
		timestamp = random.randint(timestamp, final_timestamp)
		temp_events = [{"event":"App Install", "properties":{"time":timestamp}}]
		registration_events = [("App Open",80),("Registration Complete",40)]
		completed, user = simulate_registration_funnel(registration_events, user, timestamp)
		for event in completed:
			temp_events.append(event)
			if event["event"] == "Registration Complete":
				registered = True		
		for event in temp_events:
			event["properties"].update(user["properties"])
			events.append(event)
		if registered:
			if random.randint(1,100) < 60:
				tutorial_event = {"event":"Tutorial Exited", "properties":{"time":random.randint(temp_events[-1]["properties"]["time"], temp_events[-1]["properties"]["time"]+3600)}}
				user["properties"]["Tutorial Complete"] = "True"
				user["properties"]["Tutorial Completed(%)"] = 1
				tutorial_event["properties"].update(user["properties"])
				events.append(tutorial_event)
				user["retention"] += .1
				user["conversion"] += .1
			else:
				tutorial_event = {"event":"Tutorial Exited", "properties":{"time":random.randint(temp_events[-1]["properties"]["time"], temp_events[-1]["properties"]["time"]+3600)}}
				user["properties"]["Tutorial Complete"] = "False"
				user["properties"]["Tutorial Completed(%)"] = random.choice([0, .25, .50, .75])
				tutorial_event["properties"].update(user["properties"])
				events.append(tutorial_event)
			registered_users.append(user)
			f.write(json.dumps(user) + '\n')
	f.close()
	people_batcher(registered_users, "$set")
	event_batcher(events)

def simulate_registration_funnel(events, user, timestamp):
	completed = []
	names_completed = set()
	running = True
	final_timestamp = timestamp + 86400
	while running:
		x = 0
		for event in events:
			if random.randint(1,100) < (event[1]*user["registration"]):
				if (x == 0 and x <= len(names_completed)) or (x==1 and x == len(names_completed)): 
					names_completed.add(event[0])
					timestamp = random.randint(timestamp, timestamp+3600)
					completed.append({"event":event[0], "properties":{"time":timestamp}})
					if event[0] == events[-1][0]:
						running = False
						user["people properties"]["Registration Date"] = datetime.datetime.fromtimestamp(timestamp+28800).strftime('%Y-%m-%dT%H:%M:%S')
			x += 1
		if random.randint(1,100) > (50 * user["registration_retention"]):
			running = False
		else:
			user["registration_retention"] -= .3
	return completed, user

def retained(user_file, date):
	try:
		f = open(user_file, 'r')
	except:
		f = []
	events = []
	users = []
	transactions = []
	revenue = []
	for user in f:
		user = json.loads(user)
		timestamp = calendar.timegm(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())
		required_start_events = [("App Open",30)]
		retained_events = [("Character Created", 40), ("Game Played", 70), ("Level Completed", 60), ("In-App Purchase", 20)]
		user, temp_events, temp_transactions, temp_revenue, timestamp = retention_funnel(user, required_start_events, retained_events, timestamp)
		for event in temp_events:
			events.append(event)
		for transaction in temp_transactions:
			transactions.append(transaction)
		if temp_revenue != 0:
			revenue.append(temp_revenue)
		user["people properties"]["Last Visit"] = datetime.datetime.fromtimestamp(timestamp+28800).strftime('%Y-%m-%dT%H:%M:%S')
		users.append(user)
	event_batcher(events)
	people_batcher(users, "$set")
	people_batcher(transactions, "$append")
	people_batcher(revenue, "$add")


def retention_funnel(user, required_start, events, timestamp):
	retention_events = []
	transactions = []
	timestamp = random.randint(timestamp+4000, timestamp+75400)
	running = True
	revenue = 0
	f = open('new_users.txt', 'a')
	if not user["properties"].get("Current Level"):
		user["properties"]["Current Level"] = 1
		user["people properties"]["Current Level"] = 1
	while running:
		session_started = False
		session_stats = {"games_played":0, "levels_completed":0, "in_app_purchases":0}
		for event in required_start:
			if random.randint(0,100) < event[1]*user["conversion"]:
				timestamp = random.randint(timestamp, timestamp+3600)
				start_time = timestamp
				event = {"event":event[0], "properties":{"time":timestamp}}
				event["properties"].update(user["properties"])
				retention_events.append(event)
				session_started = True
				engaged = True
			elif random.randint(0,100) > 10*user["retention"]:
				running = False
		if session_started:
			while engaged:
				for event in events:
					if random.randint(0,100) < event[1]*user["conversion"]:
						timestamp = random.randint(timestamp, timestamp+600)
						retained_event = {"event":event[0], "properties":{"time":timestamp}}
						iso_date = datetime.datetime.fromtimestamp(timestamp+28800).strftime('%Y-%m-%dT%H:%M:%S')
						if event[0] == "Game Played":
							session_stats["games_played"]+=1
							if not user["properties"].get("Total Games Played"):
								user["properties"]["Total Games Played"] = 1
								user["people properties"]["Total Games Played"] = 1
							else:
								user["properties"]["Total Games Played"] += 1
								user["people properties"]["Total Games Played"] += 1
							user["properties"]["Last Game Played"] = iso_date
							user["people properties"]["Last Game Played"] = iso_date
							retained_event["properties"].update({"Gold Earned":weighted_choice([(random.randint(1,100), 50), (random.randint(100,150), 30), (random.randint(150,200), 20), (random.randint(200,300), 10)])})
						if event[0] == "Level Completed":
							session_stats["levels_completed"]+=1
							user["properties"]["Current Level"] += 1
							user["people properties"]["Current Level"] += 1
							user["properties"]["Last Level Completed"] = iso_date
							user["people properties"]["Last Level Completed"] = iso_date
						if event[0] == "In-App Purchase":
							session_stats["in_app_purchases"]+=1
							if not user["properties"].get("Total In App Purchases"):
								user["properties"]["Total In App Purchases"] = 1
								user["people properties"]["Total In App Purchases"] = 1
							else:
								user["properties"]["Total In App Purchases"] += 1
								user["people properties"]["Total In App Purchases"] += 1
							user["properties"]["Last Purchase"] = iso_date
							user["people properties"]["Last Purchase"] = iso_date
							items = [({"Item Purchased":"Character Skin","amount":.99}, 40), ({"Item Purchased":"New Character","amount":1.99}, 20),({"Item Purchased":"Level Pack","amount":2.99}, 20), ({"Item Purchased":"Booster Pack","amount":5.99}, 10), ({"Item Purchased":"Expansion Pack","amount":12.99}, 10)]
							item = weighted_choice(items)
							retained_event["properties"].update(item)
							revenue += item["amount"]
							transactions.append({"$transactions":{"$amount":item["amount"], "$time":iso_date}, "properties":{"distinct_id":user["properties"]["distinct_id"]}})
						retained_event["properties"].update(user["properties"])
						retention_events.append(retained_event)
				if random.randint(0,100) > 50*user["retention"]:
					engaged = False
			end_time = random.randint(timestamp, timestamp+600)
			event = {"event":"Session End", "properties":{"time":end_time, "Session Time":(end_time-start_time), "Games Played This Session":session_stats["games_played"], "In-App Purchases This Session":session_stats["in_app_purchases"]}}
			session_started = False
			event["properties"].update(user["properties"])
			retention_events.append(event)
			if random.randint(0,100) > 50*user["retention"]:
				running = False
	if session_started:
		f.write(json.dumps(user)+"\n")
	elif random.randint(0,100) < 80*user["retention"]:
		f.write(json.dumps(user)+"\n")
	if revenue > 0:
		revenue = {"properties":{"distinct_id":user["properties"]["distinct_id"]}, "Revenue":revenue}
	return user, retention_events, transactions, revenue, timestamp

def stupid_file_switch(old_file, new_file):
	new = open(new_file, "r")
	old = open(old_file, "w")
	for user in new:
		old.write(user)
	old.close()
	new.close()
	new = open(new_file, "w")
	new.close()

token = "mobilegaming"
api_key = "01fc66604c6972b88b46727f51a38986"
from_date = '2014-08-01'
from_date_list = from_date.split("-")
from_date = datetime.date(int(from_date_list[0]), int(from_date_list[1]), int(from_date_list[2]))
to_date = datetime.date.today()
delta = (to_date - from_date).days

for x in range(delta):
	request_date = from_date + relativedelta.relativedelta(days=x)
	request_date = str(request_date)
	print request_date
	end_current_unix = calendar.timegm(datetime.datetime.strptime(request_date, "%Y-%m-%d").timetuple()) + 86400
	simulate_users(random.randint(2000,3000),"registered_users.txt", request_date)

