import time
# filename = 'D:\workspace\VSCode\\test\\AIS_2015_01_Zone01.csv'
# filename = '/home/workspace/geomesa/geomesa-spark/ais/AIS.csv' 
filename = '/home/workspace/geomesa/geomesa-spark/ais/AIS_2015_01_Zone01.csv'
with open(filename) as file_object:
    lines = file_object.readlines()
for line in lines:
    lineList = line.split(",")
    mmsi = lineList[0] 
    date = lineList[1]
    lat = lineList[2]
    lon = lineList[3]

    ais_temporal_log = "{mmsi}\t{date}\t{lat}\t{lon}".format(
       mmsi = mmsi,date = date,lat = lat,lon = lon
    )

    print(ais_temporal_log)
    time.sleep(1)
    # f = open('D:\workspace\VSCode\\test\\ais_temporal_log.log',"a")
    f = open('/home/workspace/geomesa/geomesa-spark/ais/ais_temporal_data.log',"a")
    f.write(ais_temporal_log+"\n")
