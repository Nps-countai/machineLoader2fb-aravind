import time
import psycopg2
import threading
import firebase_admin
from datetime import datetime
from firebase_admin import credentials, db

# Initialize Firebase Admin SDK
cred = credentials.Certificate('/home/aravinth/project/cone_json_update/coneinspection-ui18/src/pg2fbtesting.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://cone-inspection-default-rtdb.firebaseio.com/'
})

class Ctuvupdate:
    def __init__(self):
        # Connect to PostgreSQL
        self.conn = psycopg2.connect(
            host='localhost',
            database='maindb',
            user='postgres',
            password='12345'
        )
        self.cursor = self.conn.cursor()

        self.uv_Lupdatetime = self.uv_lastEntrytime()
        self.fbtip_LupdateTime = self.firebasetip_lastEntrytime()
        self.ct_Lupdatetime = self.conetip_lastEntrytime()
        self.fbuv_LupdateTime = self.firebaseuv_lastEntrytime()

        self.thread = threading.Thread(target=self.autoupdate)
        self.thread.start()

    def parse_datetime(self,datetime_str):
        try:
            if datetime_str:
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        except ValueError:
            if datetime_str:
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            else:
                return None

    def conetip_lastEntrytime(self):
        try:
            
            self.cursor.execute('SELECT id, timestamp FROM conetip order by timestamp desc limit 1')
            rows = self.cursor.fetchall()
           
            print(type(rows[0][1]),"++++++++++",rows[0][1])
            return rows[0][1]
            
        except:
            print('Check with database it-seems NODATA tip')

    def uv_lastEntrytime(self):
        try:
            
            self.cursor.execute('SELECT id, timestamp as dt FROM uv order by timestamp desc limit 1')
            row = self.cursor.fetchall()
            print(type(row[0][1]),"___________________+++",row[0][1])
            return row[0][1]
            
        except:
            print('Check with database it-seems NODATA uv')

            
    def firebasetip_lastEntrytime(self):
      
        ref = db.reference('/sudhiva/cone/')
        data = ref.get()
        if data:
            length = len(data)
            if length > 0:
                last_timestamp = data[length - 1]['timestamp']
                return last_timestamp
            else:
                print("The data is empty.")
        else:
           return "2023-03-02 00:00:00"
            

    def firebaseuv_lastEntrytime(self):
        ref = db.reference('/sudhiva/coneuv/')
        data = ref.get()
        if data:
            length = len(data)
            if length > 0:
                last_timestamp = data[length - 1]['timestamp']
                return last_timestamp
            else:
                print("The data is empty.")
        else:
           return "2023-03-02 00:00:00"


    def inifalFB_sync(self):
        print("inifalFB_sync tip loading ...")
        print('firebase_lastEntrytime : ',self.fbtip_LupdateTime)

        self.cursor.execute('SELECT id,timestamp,selectedconetype,detectedconetype,conealarmstatus,filelocation FROM conetip')
        rows = self.cursor.fetchall()
        for row in rows:
            # Update or insert data in Firebase
            ref = db.reference(f'/sudhiva/tip/{row[0]}')
            ref.set({            
                'timestamp': str(row[1]),
                'selectedconetype':row[2],
                'detectedconetype':row[3],
                'conealarmstatus' : row[4],
                'filelocation':  row[5]
            })
            print('adding data tip: ',row[0], " - ",row[1])
        print("Initial Syncing..... done")
    def inifalFBuv_sync(self):
        print("inifalFB_sync uv loading ...")
        print('firebaseuv_lastEntrytime : ',self.fbuv_LupdateTime)
        self.cursor.execute('SELECT id,timestamp,detecteduv,uvalarmstatus,filelocation,status FROM uv')
        rows = self.cursor.fetchall()
        for row in rows:
            # Update or insert data in Firebase
            ref = db.reference(f'/sudhiva/uv/{row[0]}')
            ref.set({            
                'timestamp': str(row[1]),
                'detecteduv':row[2],
                'uvalarmstatus':row[3],
                'filelocation' : row[4],
                'status':  row[5]
            })
            print('adding data uv: ',row[0], " - ",row[1])

    def autoupdate(self):
        while True:
            
            if self.fbtip_LupdateTime is None:
                self.inifalFB_sync()
                self.fbtip_LupdateTime = self.firebasetip_lastEntrytime()  
            if self.fbuv_LupdateTime is None:
                self.inifalFBuv_sync()
                self.fbuv_LupdateTime = self.firebaseuv_lastEntrytime()
            
            if self.fbtip_LupdateTime is None:
                self.fbtip_LupdateTime = datetime.min
            if self.fbuv_LupdateTime is None:
                self.fbuv_LupdateTime = datetime.min

            # self.ct_Lupdatetime = self.parse_datetime(self.conetip_lastEntrytime())
            self.uv_Lupdatetime = self.uv_lastEntrytime()
            fbtip_LupdateTime = self.parse_datetime(self.firebasetip_lastEntrytime() )
            if fbtip_LupdateTime < self.ct_Lupdatetime:
                self.cursor.execute('SELECT id, timestamp, selectedconetype, detectedconetype, conealarmstatus, filelocation FROM conetip WHERE timestamp > %s', (self.fbtip_LupdateTime,))
                rows = self.cursor.fetchall()
                for row in rows:
                    ref = db.reference(f'/sudhiva/tip/{row[0]}')
                    ref.set({
                        'timestamp': str(row[1]),
                        'selectedconetype': row[2],
                        'detectedconetype': row[3],
                        'conealarmstatus': row[4],
                        'filelocation': row[5]
                    })
                    print('adding data tip: ', row[0], " - ", row[1])
                self.ct_Lupdatetime = self.conetip_lastEntrytime()
                self.uv_Lupdatetime = self.uv_lastEntrytime()
                self.fbtip_LupdateTime = self.firebasetip_lastEntrytime()

            fbuv_LupdateTime = self.parse_datetime(self.firebaseuv_lastEntrytime())
            
            if fbuv_LupdateTime < self.uv_Lupdatetime:
                self.cursor.execute('SELECT id, timestamp, detecteduv, uvalarmstatus, filelocation, status FROM uv WHERE timestamp > %s', (fbuv_LupdateTime,))
                rows = self.cursor.fetchall()
                for row in rows:
                    ref = db.reference(f'/sudhiva/uv/{row[0]}')
                    ref.set({
                        'timestamp': str(row[1]),
                        'detecteduv': row[2],
                        'uvalarmstatus': row[3],
                        'filelocation': row[4],
                        'status': row[5]
                    })
                    print('adding data uv: ', row[0], " - ", row[1])

                self.ct_Lupdatetime = self.conetip_lastEntrytime()
                self.uv_Lupdatetime = self.uv_lastEntrytime()
                fbuv_LupdateTime = self.firebaseuv_lastEntrytime()

            else:
                print("Up to Date :)", datetime.now())

            print("------------------------------------------------")
            print("Checking every 10 seconds... syncing->->->")
            time.sleep(10)  # Changed sleep time to 10 seconds

class Alarmstatusupdate:
    def __init__(self):

        # Connect to PostgreSQL
        self.conn = psycopg2.connect(
            host='localhost',
            database='tipreport1',
            user='postgres',
            password='12345'
        )
        self.cursor = self.conn.cursor()
        self.uvalarm_Lupdatetime = self.uvalarm_lastEntrytime()
        self.fbtip_LupdateTime = self.firebasetip_lastEntrytime()
        self.ctalarm_Lupdatetime = self.ctalarm_lastEntrytime()
        self.fbuv_LupdateTime = self.firebaseuv_lastEntrytime()

        self.thread = threading.Thread(target=self.autoupload)
        self.thread.start()

    def parse_datetime(self,datetime_str):
        try:
            if datetime_str:
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        except ValueError:
            if datetime_str:
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            else:
                return None

    def uvalarm_lastEntrytime(self):
        try:
            self.cursor.execute('SELECT id, time FROM uvreport ORDER BY time DESC LIMIT 1')
            conetip_row = self.cursor.fetchone()

            return conetip_row[1]
        except:
            print('Check with database it-seems NODATA')

    def ctalarm_lastEntrytime(self):
        try:
            
            self.cursor.execute('SELECT id, start_time FROM ctreports ORDER BY start_time DESC LIMIT 1')
            conetip_row = self.cursor.fetchone()
            return conetip_row[1]
        except:
            print('Check with database it-seems NODATA')

    def firebasetip_lastEntrytime(self):
        try:
            ref = db.reference('/sudhiva/tipalarm/')
            length = len(ref.get())
            return ref.get()[length-1]['timestamp']
        except Exception as e:
            print("An exception occurred",e)

    def firebaseuv_lastEntrytime(self):
        try:
            ref = db.reference('/sudhiva/uvalarm/')
            length = len(ref.get())
            return ref.get()[length-1]['timestamp']
        except Exception as e:
            print("An exception occurred",e) 


    def inifalFB_sync(self):
        print("inifalFB_sync loading ...")
        print('firebase_lastEntrytime : ',self.fbtip_LupdateTime)
        
        self.cursor.execute('SELECT id,selected_conetype,conetip_scanned,start_time,end_time,alarm_status FROM ctreports')
        rows = self.cursor.fetchall()
        for row in rows:
            # Update or insert data in Firebase

            ref = db.reference(f'/sudhiva/tipalarm/{row[0]}')
            ref.set({            
                'selected_conetype': str(row[1]),
                'conetip_scanned':row[2],
                'start_time':str(row[3]),
                'end_time' : str(row[4]),
                'alarm_status':  row[5]
            })
            print('adding data tipalarm : ',row[0], " - ",row[1])
    def inifalFBuv_sync(self):
        print("inifalFB_sync loading ...")
        print('firebase_lastEntrytime : ',self.fbuv_LupdateTime)
        self.cursor.execute('SELECT id,cones_scanned,time,image,alarm_status FROM uvreport')
        rows = self.cursor.fetchall()
        for row in rows:
            # Update or insert data in Firebase
            ref = db.reference(f'/sudhiva/uvalarm/{row[0]}')
            ref.set({            
                'conetip_scanned': str(row[1]),
                'time':str(row[2]),
                'image':row[3],
                'alarmstatus' : row[4],

            })
            print('adding data uvalarm : ',row[0], " - ",row[1])
    
    def autoupload(self):
        while True:
            
            if not self.fbtip_LupdateTime:
                self.inifalFB_sync()
                self.fbtip_LupdateTime = self.firebasetip_lastEntrytime()  
            if not self.fbuv_LupdateTime:
                self.inifalFBuv_sync()
                self.fbuv_LupdateTime = self.firebaseuv_lastEntrytime() 
            
            self.fbtip_LupdateTime = self.parse_datetime(self.firebasetip_lastEntrytime())
            print("uvalarm_last Entry time : ",self.uvalarm_Lupdatetime)
            print("firebase_last cone Entry time : ",self.fbtip_LupdateTime)
            print("ctalarm_last Entry time : ",self.ctalarm_Lupdatetime)
            

            if self.fbtip_LupdateTime < self.ctalarm_lastEntrytime():
                self.cursor.execute('SELECT id,selected_conetype,conetip_scanned,start_time,end_time,alarm_status FROM ctreports WHERE start_time > %s', (self.fbtip_LupdateTime,))
                rows = self.cursor.fetchall()
                for row in rows:
                    
                    # Update or insert data in Firebase
                    ref = db.reference(f'/sudhiva/tipalarm/{row[0]}')
                    ref.set({            
                        'selected_conetype': str(row[1]),
                        'conetip_scanned':row[2],
                        'start_time':str(row[3]),
                        'end_time' : str(row[4]),
                        'alarm_status':  row[5]
                    })
                    print('adding data : ',row[0], " - ",row[1])
                self.fbtip_LupdateTime = self.parse_datetime(self.firebasetip_lastEntrytime())
                self.uvalarm_Lupdatetime = self.uvalarm_lastEntrytime()
                self.ctalarm_Lupdatetime = self.ctalarm_lastEntrytime()
                self.fbuv_LupdateTime = self.firebaseuv_lastEntrytime()

            self.fbuv_LupdateTime = self.parse_datetime(self.firebaseuv_lastEntrytime())
            print("uvalarm_last Entry time : ",self.uvalarm_Lupdatetime)
            print("firebase_last cone Entry time : ",self.fbtip_LupdateTime)
            print("ctalarm_last Entry time : ",self.ctalarm_Lupdatetime)
            print("firebase_last uv Entry time",self.fbuv_LupdateTime)
            if self.fbuv_LupdateTime < self.uvalarm_lastEntrytime():
            
                self.cursor.execute('SELECT id, cones_scanned, time, image, alarm_status FROM uvreport WHERE time > %s', (self.fbuv_LupdateTime,))
                rows = self.cursor.fetchall()
                for row in rows:
                    # Update or insert data in Firebase
                    
                    ref = db.reference(f'/sudhiva/uvalarm/{row[0]}')
                    ref.set({            
                        'conetip_scanned': str(row[1]),
                        'time':str(row[2]),
                        'image':row[3],
                        'alarmstatus' : row[4],

                    })
                    print('adding data : ',row[0], " - ",row[1])
                self.fbuv_LupdateTime = self.firebaseuv_lastEntrytime()
                self.uvalarm_Lupdatetime = self.uvalarm_lastEntrytime()
                self.ctalarm_Lupdatetime = self.ctalarm_lastEntrytime()
            else:
                print("Upto Date :)", datetime.now())
            time.sleep(0.001)

# run = Alarmstatusupdate()
# run.autoupload()

run1 = Ctuvupdate()
run1.autoupdate()