#coding:utf-8
import string
import random
import time

# randmon name
def name():
    return random.choice(string.ascii_uppercase)+''.join(random.choice(string.ascii_lowercase) for _ in range(4))
   # return random.randint(0,100)

# random lon
def longitude():
    return "%.6f" % random.uniform(-180,180)

# random lat
def latitude():
    return "%.6f" % random.uniform(-90,90)

# localtime
def date():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# merge all data
def spatio_temporal_data():
	# f = open("D:\data\geomesa\\spatio_temporal_data.log","w+")
    f = open("/home/data/geomesa/spatio_temporal_data.log","w+")
    
    while True:
        spatio_temporal_log = "{name}\t{longitude}\t{latitude}\t{date}".format(
            name = name(),longitude=longitude(),latitude=latitude(),date = date()
        )
        
        f.write(spatio_temporal_log + "\n")
        print(spatio_temporal_log)
        time.sleep(1)

# merge all data，每一100条
def spatio_temporal_data_100():
	# f = open("D:\data\geomesa\\spatio_temporal_data.log","w+")
    f = open("/home/workspace/geomesa/geomesa-spark/person/spatio_temporal_data.log","a")
    while True:
        spatio_temporal_log = "{name}\t{longitude}\t{latitude}\t{date}".format(
            name = name(),longitude=longitude(),latitude=latitude(),date = date()
        )
        
        f.write(spatio_temporal_log + "\n")
        print(spatio_temporal_log)
        time.sleep(1)        

if __name__ == "__main__":
    # spatio_temporal_data()
    spatio_temporal_data_100()
