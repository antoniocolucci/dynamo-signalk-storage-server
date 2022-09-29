import pyodbc
import json 
import datetime

def store_updatelist_csv(update_list, options):
    csv_root=options["csv_root"]
    for update_item in update_list:
        context=update_item["context"]
        updates=update_item["updates"]
        for update in updates:
            timestamp=update["timestamp"]
            values=update["values"]
            for value in values:
                path=value["path"]
                value=value["value"]
                csv_path=csv_root+"/"+path+".csv"

                if isinstance(value,dict):
                    value=json.dumps(value)

                if isinstance(value, basestring):
                    value="'"+value+"'"

                row=context+";"+timestamp+";"+str(value)+"\n"
                with open(csv_path,'a') as fd:
                    fd.write(row)


def store_updatelist_odbc(update_list, options):
    connection_string=options["connection_string"]
    con = pyodbc.connect(connection_string)

    for update_item in update_list:
        context=update_item["context"]
        updates=update_item["updates"]
        for update in updates:
            timestamp = datetime.datetime.strptime(update["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ')            
            values=update["values"]
            for value in values:
                path=value["path"]
                if path is None or path=="":
                    break

                value=value["value"]
                table_name = path.replace(".","_")

                cur = con.cursor()
                sql_string = "SELECT CONTEXT FROM "+table_name
                try:
                    #print sql_string
                    cur.execute(sql_string)
                except Exception as e:
                    print(e)

                    cur = con.cursor()

                    if path == "navigation.position":
                        sql_string = "CREATE TABLE "+table_name+" (context text NOT NULL, timestamp datetime NOT NULL, value text, lon real, lat real, point geometry, PRIMARY KEY (context, timestamp))"
                    else:
                        if isinstance(value,dict):
                            value_datatype="text"
                        elif isinstance(value, basestring):  
                            value_datatype="text"
                        else:
                            value_datatype="real"

                        sql_string = "CREATE TABLE "+table_name+" (context text NOT NULL, timestamp datetime NOT NULL, value " + value_datatype + ", PRIMARY KEY (context, timestamp))"

                    #print sql_string
                    cur.execute(sql_string)
                    con.commit()

                cur = con.cursor()

                if path == "navigation.position":
                    params=(context, timestamp, json.dumps(value), value["longitude"], value["latitude"], "POINT ("+str(value["latitude"])+" "+str(value["longitude"])+")")
                    sql_string = "insert into " + table_name + "(context, timestamp, value, lon, lat, point) values (?, ?, ?, ?, ?, ?)"
                else:
                    if isinstance(value,dict):
                        value=json.dumps(value)

                    params=(context, timestamp, value)
                    sql_string = "insert into " + table_name + "(context, timestamp, value) values (?, ?, ?)"

                #print sql_string
                #print params
                try:
                    cur.execute(sql_string, params)
                    con.commit()
                except Exception as e:
                    print(e)


def main():
    options={ "connection_string":"DRIVER=SQLite3;SERVER=localhost;DATABASE=test.db;Trusted_connection=yes"}
    update_list=[
        {
            "context":"vessels.urn:mrn:signalk:uuid:d8cf5bc8-ad83-4269-baac-184cf12e3d79",
            "updates":[
                {
                    "source":{"src":"derived_data","label":"derived-data"},
                    "timestamp":"2019-08-20T03:41:46.606Z",
                    "values":[
                        {
                            "path":"navigation.courseOverGroundMagnetic",
                            "value":4.032583237653657
                        }
                    ]
                }
            ]
        },
        {
            "updates":[
                {
                    "source":{"sentence":"RMC","talker":"GP","type":"NMEA0183","label":"USB1"},
                    "timestamp":"2019-08-20T03:41:46.000Z",
                    "values":[
                        {
                            "path":"navigation.position",
                            "value":{"longitude":14.029045,"latitude":40.76749}
                        },
                        {
                            "path":"navigation.courseOverGroundTrue",
                            "value":4.032583237653657
                        },
                        {
                            "path":"navigation.speedOverGround",
                            "value":0
                        },
                        {
                            "path":"navigation.magneticVariation",
                            "value":0
                        },
                        {
                            "path":"navigation.magneticVariationAgeOfService",
                            "value":1566272506
                        },
                        {
                            "path":"navigation.datetime",
                            "value":"2019-08-20T03:41:46.000Z"
                        }
                    ]
                }
            ],
            "context":"vessels.urn:mrn:signalk:uuid:d8cf5bc8-ad83-4269-baac-184cf12e3d79"
        },
        {
            "updates":[
                {
                    "source":{"sentence":"GGA","talker":"GP","type":"NMEA0183","label":"USB1"},
                    "timestamp":"2019-08-20T03:41:47.000Z",
                    "values":[
                        {
                            "path":"navigation.position",
                            "value":{"longitude":14.029045,"latitude":40.76749}
                        },
                        {
                            "path":"navigation.gnss.methodQuality",
                            "value":"GNSS Fix"
                        },
                        {
                            "path":"navigation.gnss.satellites",
                            "value":10
                        },
                        {
                            "path":"navigation.gnss.antennaAltitude",
                            "value":6
                        },
                        {
                            "path":"navigation.gnss.horizontalDilution",
                            "value":1
                        },
                        {
                            "path":"navigation.gnss.differentialAge",
                            "value":0
                        },
                        {
                            "path":"navigation.gnss.differentialReference",
                            "value":0
                        }
                    ]
                }
            ],
            "context":"vessels.urn:mrn:signalk:uuid:d8cf5bc8-ad83-4269-baac-184cf12e3d79"
        }
    ]
    
    store_updatelist_odbc(update_list, options)

    connection_string=options["connection_string"]
    con = pyodbc.connect(connection_string)
    cur = con.cursor()
    sql_string="SELECT * FROM navigation_position ORDER BY timestamp"
    rows=cur.execute(sql_string).fetchall()
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
