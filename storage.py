from sqlalchemy import create_engine, Column, Table, JSON, Text, Float, MetaData, DateTime, select
from geoalchemy2 import Geometry

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

                if isinstance(value, str):
                    value="'"+value+"'"

                row=context+";"+timestamp+";"+str(value)+"\n"
                with open(csv_path,'a') as fd:
                    fd.write(row)


def store_updatelist(update_list, options):
    connection_string=options["connection_string"]
    engine = create_engine(connection_string, echo=False)
    metadata = MetaData()

    for update_item in update_list:
        context=update_item["context"]
        updates=update_item["updates"]
        for update in updates:
            timestamp = datetime.datetime.strptime(update["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ')
            source = json.dumps(update["source"])
            values=update["values"]
            for value in values:
                data_table=None

                path=value["path"]
                if path is None or path=="":
                    break

                value=value["value"]
                table_name = path.replace(".", "_")

                try:
                    data_table = metadata.tables[table_name]

                except Exception as e:
                    print(e)


                    if path == "navigation.position":
                        data_table = Table(table_name, metadata,
                                           Column('context', Text, nullable=False, primary_key=True),
                                           Column('timestamp', DateTime, nullable=False, primary_key=True),
                                           Column('source', Text, nullable=False, primary_key=True),
                                           Column('value', JSON),
                                           Column('lon', Float),
                                           Column('lat', Float),
                                           Column('point', Geometry('POINT'))
                        )
                    else:
                        if isinstance(value, dict):
                            value_datatype = JSON
                        elif isinstance(value, str):
                            value_datatype = Text
                        else:
                            value_datatype = Float

                        data_table = Table(table_name, metadata,
                                           Column('context', Text, nullable=False, primary_key=True),
                                           Column('timestamp', DateTime, nullable=False, primary_key=True),
                                           Column('source', Text, nullable=False, primary_key=True),
                                           Column('value', value_datatype)
                        )
                    #print sql_string
                    metadata.create_all(engine)

                if data_table is not None:
                    if path == "navigation.position":
                        params = {
                            "context": context,
                            "timestamp": timestamp,
                            "source": source,
                            "value": value,
                            "lon": value["longitude"],
                            "lat": value["latitude"],
                            "point": "POINT ("+str(value["latitude"])+" "+str(value["longitude"])+")"
                        }
                    else:
                        if isinstance(value,dict):
                            value = json.dumps(value)

                        params = {
                            "context": context,
                            "timestamp": timestamp,
                            "source": source,
                            "value": value
                        }

                    sql_string = data_table.insert().values(params)

                    try:
                        conn = engine.connect()
                        conn.execute(sql_string)
                        conn.close()
                    except Exception as e:
                        print(e)


def main():
    # sqlite:///temporary.db
    # postgresql://dynamo:Dynamo2019@localhost/dynamo
    options = {"connection_string": "postgresql://dynamo:Dynamo2019@localhost/dynamo"}
    update_list = [
        {
            "context": "vessels.urn:mrn:signalk:uuid:d8cf5bc8-ad83-4269-baac-184cf12e3d79",
            "updates": [
                {
                    "source": {"src": "derived_data","label": "derived-data"},
                    "timestamp": "2019-08-20T03:41:46.606Z",
                    "values": [
                        {
                            "path": "navigation.courseOverGroundMagnetic",
                            "value": 4.032583237653657
                        }
                    ]
                }
            ]
        },
        {
            "updates": [
                {
                    "source": {"sentence": "RMC", "talker": "GP", "type": "NMEA0183", "label": "USB1"},
                    "timestamp": "2019-08-20T03:41:46.000Z",
                    "values": [
                        {
                            "path": "navigation.position",
                            "value": {"longitude":14.029045,"latitude":40.76749}
                        },
                        {
                            "path": "navigation.courseOverGroundTrue",
                            "value": 4.032583237653657
                        },
                        {
                            "path": "navigation.speedOverGround",
                            "value": 0
                        },
                        {
                            "path": "navigation.magneticVariation",
                            "value": 0
                        },
                        {
                            "path": "navigation.magneticVariationAgeOfService",
                            "value": 1566272506
                        },
                        {
                            "path": "navigation.datetime",
                            "value": "2019-08-20T03:41:46.000Z"
                        }
                    ]
                }
            ],
            "context": "vessels.urn:mrn:signalk:uuid:d8cf5bc8-ad83-4269-baac-184cf12e3d79"
        },
        {
            "updates": [
                {
                    "source": {"sentence": "GGA", "talker": "GP", "type": "NMEA0183", "label": "USB1"},
                    "timestamp": "2019-08-20T03:41:47.000Z",
                    "values": [
                        {
                            "path": "navigation.position",
                            "value": {"longitude": 14.029045,"latitude": 40.76749}
                        },
                        {
                            "path": "navigation.gnss.methodQuality",
                            "value": "GNSS Fix"
                        },
                        {
                            "path": "navigation.gnss.satellites",
                            "value": 10
                        },
                        {
                            "path": "navigation.gnss.antennaAltitude",
                            "value": 6
                        },
                        {
                            "path": "navigation.gnss.horizontalDilution",
                            "value": 1
                        },
                        {
                            "path": "navigation.gnss.differentialAge",
                            "value": 0
                        },
                        {
                            "path": "navigation.gnss.differentialReference",
                            "value": 0
                        }
                    ]
                }
            ],
            "context": "vessels.urn:mrn:signalk:uuid:d8cf5bc8-ad83-4269-baac-184cf12e3d79"
        }
    ]
    
    store_updatelist(update_list, options)

    connection_string=options["connection_string"]

    engine = create_engine(connection_string, echo=True)
    conn = engine.connect()
    metadata = MetaData(bind=conn, reflect=True)
    navigation_position = metadata.tables["navigation_position"]
    s = select([navigation_position])
    rows = conn.execute(s)
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
