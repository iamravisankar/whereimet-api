import json
from flask import Flask, jsonify, request, redirect
from flask_cors import CORS
import mysql.connector
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import urllib.request
from datetime import datetime
import time
from flask.json import JSONEncoder
from bson import json_util

class MiniJSONEncoder(JSONEncoder):
    """Minify JSON output."""
    item_separator = ','
    key_separator = ':'

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="ravi",
  database="covid"
)
mycursor = mydb.cursor()
userid = 0;
uservisited= []
app = Flask(__name__)
app.json_encoder = MiniJSONEncoder
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
CORS(app)
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET', 'POST'])
def initial():
    print("ravi")
    if request.method == 'POST':
        query_parameters = request.args
        receivedata = request.form['data']
        newdata = json.loads(receivedata)
        print(newdata['method'])
        if newdata['method'] == "insert":
            return getdata()
        elif newdata['method'] == "detect":
            detect()
            return detect()
        elif newdata['method'] == "all":
            return others()
        elif newdata['method'] == "getreports":
            return getreports()
        elif newdata['method'] == "getusers":
            return getusers()
        elif newdata['method'] == "reportbyid":
            return reportbyid()
    else:
        print("--------")
        if request.method == 'GET':
            query_parameters = request.args
            print(request.form)
            type = request.args.get('type')
            if type == 'bulk' :
                print("-yyyyyyy")
                return scanall()
            else :
                print("zzzzzzzzzzz")
                return "whooohooo"
            # receivedata = request.form['data']
            # newdata = json.loads(receivedata)
            # print(newdata['method'])
            # if newdata['method'] == "insert":
            #     return getdata()
            # elif newdata['method'] == "detect":
            #     detect()
            #     return detect()
        else:

            return "auth"
    return "auth"
def getdata():
    # Check if a valid image file was uploaded

    if request.method == 'POST':
        query_parameters = request.args
        receivedata = request.form['data']
        print(receivedata)
        # print(receivedata.image)
        newdata = json.loads(receivedata)
        print(newdata['email'])
        newuserid = register(newdata['gid'], newdata['fullname'], newdata['givenname'], newdata['familyname'], newdata['image'], newdata['email'])
        global userid
        userid = newuserid
        print(newuserid)
        if newuserid == 0 :
            print("already")
            sql = "SELECT userid FROM `users` WHERE gid = " + newdata['gid']
            mycursorget = mydb.cursor()
            mycursorget.execute(sql)
            olduserid = mycursorget.fetchall()
            result = {
               "status": "success", "userid": olduserid[0][0] 
            }
        else:
            jsonarray = ziptojsonarray(newdata['filelink'])
            insertedcount = jsontosql(jsonarray)
            print(insertedcount)
            result = {
            "status": "success", "userid": userid
             } 

    return jsonify(result)


def register(gid,fullname,givenname,familyname,image,email):
    sql = "INSERT IGNORE INTO `users` ( `gid`, `fullname`, `givenname`, `familyname`, `image`, `email`) VALUES (%s, %s, %s, %s, %s, %s)"
    # sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = (gid, fullname, givenname, familyname , image, email)
    mycursor.execute(sql, val)
    mydb.commit()

    print("1 record inserted, ID:", mycursor.lastrowid)
    return mycursor.lastrowid

def ziptojsonarray(url):
    jsonarray = []
    url = urllib.request.urlopen(url)
    with ZipFile(BytesIO(url.read())) as my_zip_file:
        for contained_file in my_zip_file.namelist():
            # print(contained_file)
            if contained_file == "Takeout/Location History/Semantic Location History/2020/2020_JANUARY.json" or contained_file == "Takeout/Location History/Semantic Location History/2020/2020_FEBRUARY.json" or contained_file == "Takeout/Location History/Semantic Location History/2020/2020_MARCH.json" or contained_file == "Takeout/Location History/Semantic Location History/2020/2020_APRIL.json":
                # print("yes")
                # with open(("unzipped_and_read_" + contained_file + ".file"), "wb") as output:
                findata = my_zip_file.open(contained_file).read().decode("utf-8")
                # print(findata.replace('\n', ''))
                jsonarray.append(findata.replace('\n', ''))
    return jsonarray

def jsontosql(jsonarray):
     sql = "INSERT INTO `locations` (`userid`, `placeid`, `timefrom`, `timeto`, `lat`, `lng`, `address`, `addressname`, `confidence`) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s)"
     newdata = []
     for jns in jsonarray:
         # print(jns)
         data = json.loads(jns)
         for p in data['timelineObjects']:
                try:
                    p['placeVisit']
                except:
                 # handle this
                    a = 0
                else:
                    # print("sure, it was defined.")
                         try:
                           p['placeVisit']['location']['name']
                         except:
                           # handle this
                           addressname="none"
                         else:
                             addressname = p['placeVisit']['location']['name']
                             try:
                                 p['placeVisit']['location']['address']
                             except:
                                 # handle this
                                 address = "none"
                             else:
                                address = p['placeVisit']['location']['address']
                                # print(p['placeVisit']['location']['placeId'])
                                if p['placeVisit']['visitConfidence'] > 60:
                                   newdata.append((userid,p['placeVisit']['location']['placeId'],p['placeVisit']['duration']['startTimestampMs'],p['placeVisit']['duration']['endTimestampMs'],p['placeVisit']['location']['latitudeE7'],p['placeVisit']['location']['longitudeE7'],address,addressname,p['placeVisit']['visitConfidence']))
                                #break
     mycursor.executemany(sql, newdata)
     mydb.commit()

     return mycursor.rowcount


def detect():
    query_parameters = request.args
    receivedata = request.form['data']
    newdata = json.loads(receivedata)
    mycursor = mydb.cursor()
    mycursor2 = mydb.cursor()
    mycursor99 = mydb.cursor()
    mycursorp = mydb.cursor()
    sqlp = "SELECT * FROM `locations` WHERE suspect = 1"
    mycursorp.execute(sqlp)
    if mycursorp.rowcount: 
        allpatients = mycursorp.fetchall()
    else: 
        allpatients = []
    userplaces = []
    matchedplaces = []
    temparray = []
    sql99 = "SELECT * FROM `locations` WHERE userid = " + format(newdata['userid'])
    mycursor99.execute(sql99)
    if mycursor99.rowcount: 
        global uservisited
        uservisited = mycursor99.fetchall()
        sql = "SELECT placeid FROM `locations` WHERE userid = " + format(newdata['userid'])
        mycursor.execute(sql)
        if mycursor.rowcount: 
            userfull = mycursor.fetchall()
            for x in userfull:
                # print("this is id " + x[0])
                userplaces.append((x[0]))
            format_strings = ','.join(['%s'] * len(userplaces))
            mycursor.execute("SELECT * FROM `locations` WHERE suspect = 1 AND `placeid` IN (%s)" % format_strings,
                            tuple(userplaces))
            if mycursor.rowcount:                
                suspectdata = mycursor.fetchall()
                print(suspectdata)
                matchedplaces.append('')
                for x in suspectdata:
                    # print(x[2])
                    matchedplaces.append((x[2]))
                format_strings = ','.join(['%s'] * len(matchedplaces))
                mycursor2.execute("SELECT * FROM `locations` WHERE userid = " + format(newdata['userid']) +" AND `placeid` IN (%s) ORDER BY timefrom DESC" % format_strings,
                                tuple(matchedplaces))
                print("sds")                
                if mycursor2.rowcount:
                    userdata = mycursor2.fetchall()
                    print(userdata)
                    print("______________**____________")
                    print(suspectdata)

                    for x in userdata:
                        print("sds")
                        for y in suspectdata:
                            print(x[2])
                            print(y[2])
                            print("__________________________")
                            if x[2] == y[2]:
                                # print(x[4])
                                # print(int(x[4]))
                                global datetime
                                xd = datetime.fromtimestamp(int(x[4]) / 1000)
                                yd = datetime.fromtimestamp(int(y[4]) / 1000)
                                if xd.date() == yd.date():
                                    print("You and patient went to ", x[8], "on", xd.date())
                                    print("You")
                                    xandy = [x,y]
                                    temparray.append((xandy))
                                    # from datetime import datetime
                                    # timestamp = 1545730073
                                    # dt_object = datetime.fromtimestamp(timestamp)
                                    #
                                    # print("dt_object =", dt_object)
                else:
                    temparray['status']= "nomatch";
            else:
                    temparray['status']= "nomatch";
        else :
            temparray['status']= "nomatch";
        newtemp=[]
        newtemp.append(temparray)
        newtemp.append(uservisited)
        newtemp.append(allpatients)
        return json.dumps(newtemp)
    else :
        return [] 
        print("stoooooooooooooooop")

def scanall():
    query_parameters = request.args
    # receivedata = request.form['data']
    # newdata = json.loads(receivedata)
    mycursor = mydb.cursor()
    mycursor2 = mydb.cursor()
    mycursor99 = mydb.cursor()
    mycursorxyxy = mydb.cursor()
    userplaces = []
    matchedplaces = []
    temparray = []
    newreportdata = []
    a1 = 'null'
    a2 = 'null'
    sql = "INSERT IGNORE INTO `reports` ( `totalscanned`, `withpatient`) VALUES (%s, %s)"
    val = (a1,a2)
    mycursorxyxy.execute(sql, val)
    mydb.commit()

    print("1 record inserted, ID:", mycursorxyxy.lastrowid)
    reportid =  mycursorxyxy.lastrowid
    sql99 = "SELECT * FROM `locations`" 
    # print("0000000000000000000")
    mycursor99.execute(sql99)
    if mycursor99.rowcount: 
        global uservisited
        uservisited = mycursor99.fetchall()
    else :
        a = 0
    sql = "SELECT placeid FROM `locations`"
    mycursor.execute(sql)
    # print("0000000000000000000")
    if mycursor.rowcount: 
        userfull = mycursor.fetchall()
        for x in userfull:
            # print("this is id " + x[0])
            userplaces.append((x[0]))
        format_strings = ','.join(['%s'] * len(userplaces))
        mycursor.execute("SELECT * FROM `locations` WHERE suspect = 1 AND `placeid` IN (%s)" % format_strings,
                        tuple(userplaces))
        if mycursor.rowcount:                
            suspectdata = mycursor.fetchall()
            # print(suspectdata)
            matchedplaces.append('')
            for x in suspectdata:
                # print(x[2])
                matchedplaces.append((x[2]))
            format_strings = ','.join(['%s'] * len(matchedplaces))
            mycursor2.execute("SELECT * FROM `locations` WHERE  `placeid` IN (%s) ORDER BY timefrom DESC" % format_strings,
                            tuple(matchedplaces))
            # print("sds")                
            if mycursor2.rowcount:
                userdata = mycursor2.fetchall()
                # print(userdata)
                # print("______________**____________")
                # print(suspectdata)

                for x in userdata:
                    print("sds")
                    for y in suspectdata:
                        # print(x[2])
                        # print(y[2])
                        # print("__________________________")
                        if x[2] == y[2] and  x[1] != y[1]:
                            # print(x[4])
                            # print(int(x[4]))
                            global datetime
                            xd = datetime.fromtimestamp(int(x[4]) / 1000)
                            yd = datetime.fromtimestamp(int(y[4]) / 1000)
                            if xd.date() == yd.date():
                                print("You and patient went to ", x[8], "on", xd.date())
                                # print("You")
                                xandy = [x,y]
                                temparray.append((xandy))
                                newreportdata.append((x[1],y[1],x[2],x[3],x[4],y[3],y[4],x[5],x[6],x[7],x[8],x[9],y[9],reportid))
                                # from datetime import datetime
                                # timestamp = 1545730073
                                # dt_object = datetime.fromtimestamp(timestamp)
                                #
                                # print("dt_object =", dt_object)
            else:
                temparray['status']= "nomatch";
        else:
                temparray['status']= "nomatch";
    else :
        temparray['status']= "nomatch";
    mycursorreport = mydb.cursor()
    sql = "INSERT INTO `reportdata` (`userid`, `suspectid`, `placeid`, `utimefrom`, `utimeto`, `ptimefrom`, `ptimeto`, `lat`, `lng`, `address`, `addressname`, `uconfidence`, `pconfidence`, `reportid`)  VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)"
    # sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    #  mycursor.executemany(sql, newdata)
    mycursorreport.executemany(sql, newreportdata)
    mydb.commit()

    # print("1 record inserted, ID:", mycursorreport.lastrowid)
    # return mycursorreport.lastrowid
    # return json.dumps(newtemp)
    return "insterted"

def others():
    query_parameters = request.args
    receivedata = request.form['data']
    newdata = json.loads(receivedata)
    mycursorp = mydb.cursor()
    sqlp = "SELECT * FROM `locations` WHERE suspect = 1"
    allpatients = []
    newtemp = []
    mycursorp.execute(sqlp)
    if mycursorp.rowcount: 
        allpatients = mycursorp.fetchall()
    else: 
        allpatients = []
    newtemp.append(allpatients)
    return json.dumps(newtemp)
    # return "anujj"


def getreports():
    query_parameters = request.args
    receivedata = request.form['data']
    newdata = json.loads(receivedata)
    mycursorp = mydb.cursor(dictionary=True)
    sqlp = "SELECT * FROM `reports`"
    allpatients = []
    newtemp = []
    mycursorp.execute(sqlp)
    if mycursorp.rowcount: 
        allpatients = mycursorp.fetchall()
    else: 
        allpatients = []
    newtemp.append(allpatients)
    return json.dumps(newtemp, default=json_util.default)
    # return "anujj"

def getusers():
    query_parameters = request.args
    receivedata = request.form['data']
    newdata = json.loads(receivedata)
    mycursorp = mydb.cursor(dictionary=True)
    sqlp = "SELECT * FROM `users`"
    allpatients = []
    newtemp = []
    mycursorp.execute(sqlp)
    if mycursorp.rowcount: 
        allpatients = mycursorp.fetchall()
    else: 
        allpatients = []
    newtemp.append(allpatients)
    return json.dumps(newtemp)
    # return "anujj"


def reportbyid():
    query_parameters = request.args
    receivedata = request.form['data']
    newdata = json.loads(receivedata)
    mycursorp = mydb.cursor(dictionary=True)
    sqlp = "SELECT * FROM `reportdata` WHERE reportid = " + format(newdata['reportid'])
    allpatients = []
    newtemp = []
    mycursorp.execute(sqlp)
    if mycursorp.rowcount: 
        allpatients = mycursorp.fetchall()
    else: 
        allpatients = []
    newtemp.append(allpatients)
    return json.dumps(newtemp)
    # return "anujj"



if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
