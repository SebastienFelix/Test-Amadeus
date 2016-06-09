import sys
from pyspark import SparkContext


def get_arrival_airport_bookings(line):
	line_split=line.split("^")
	if len(line_split)<35:
		airport=line_split[10]
		return airport.split(" ")[0]
	airport=line_split[12]
	return airport.split(" ")[0]

def get_departure_airport_bookings(line):
	line_split=line.split("^")
	if len(line_split)<35:
		airport=line_split[7]
		return airport.split(" ")[0]
	airport=line_split[9]
	return airport.split(" ")[0]

def get_date_bookings(line):
	line_split=line.split("^")
	if len(line_split)<35:
		airport=line_split[28]
		return airport.split(" ")[0]
	airport=line_split[32]
	return airport.split(" ")[0]

def get_arrival_airport_searches(line):
	line_split=line.split("^")
	if len(line_split)<7:
		line_bis=line_split[0].split(",")
		return line_bis[6]
	return line_split[6]

def get_departure_airport_searches(line):
	line_split=line.split("^")
	if len(line_split)<6:
		line_bis=line_split[0].split(",")
		return line_bis[5]
	return line_split[5]

def get_date_searches(line):
	line_split=line.split("^")
	if len(line_split)<12:
		line_bis=line_split[0].split(",")
		if len(line_bis)<12:
			print("line=",line)
			print("line_bis=",line_bis)
			input("error")
		else:
			if line_bis[11]=='':
				return line_bis[0]
			#return line_bis[11]
			return line_bis[0]
	if line_split[11]=='':
		return line_split[0]
	#return line_split[11]
	return line_split[0]

def is_valid_line_bookings(line):
	line_split=line.split("^")
	if len(line_split)>=35:
		if line_split[34].split(" ")[0]=='pax':
			return False
		else:
			return True
	else:
		if line_split[len(line_split)-4].split(" ")[0]=='pax':
			return False
		else:
			return True

def is_valid_line_searches(line):
	line_split=line.split("^")
	if line_split[0]=='Date':
		return False
	else:
		if len(line_split)<12:
			line_bis=line_split[0].split(",")
			if len(line_bis)<12:
				print("!!!! BAD LINE=",line)
				return False
			else:
				return True
		else:
			return True

def key_value_to_line(pair):
	sch=pair[0].split("^")
	vle=pair[1]
	return sch[0]+" "+sch[1]+" "+sch[2]+" "+str(vle)
    


if __name__=="__main__":
	if len(sys.argv) < 2:
		print >> sys.stderr, "Usage: Exercise BONUS <file>"
		exit(-1)
        
	sc=SparkContext()

	files=sys.argv[1].split(',')

	bookings=files[0]
	searches=files[1]
    
	data_bookings=sc.textFile(bookings)
	data_searches=sc.textFile(searches)

	data_bookings_filtered=data_bookings.filter(lambda line: is_valid_line_bookings(line))
	data_searches_filtered=data_searches.filter(lambda line: is_valid_line_searches(line))

	bookings_data_final=data_bookings_filtered.map(lambda line: (get_date_bookings(line)+"^"+get_departure_airport_bookings(line)+"^"+get_arrival_airport_bookings(line),1)).distinct()
	searches_data_final=data_searches_filtered.map(lambda line: (get_date_searches(line)+"^"+get_departure_airport_searches(line)+"^"+get_arrival_airport_searches(line),0)).distinct()

	bdf10=bookings_data_final.top(100)
	print("top10 bookings= ",bdf10)
	sdf10=searches_data_final.top(100)
	print("top10 searches= ",sdf10)

	data_final=bookings_data_final.union(searches_data_final)
	data_final_combined=data_final.reduceByKey(lambda v1,v2:v1+v2)
	
	data_final_top10=data_final_combined.top(100)
	print("top10 data final= ",data_final_top10)

	data_final_collect=data_final_combined.collect()
	data_final_csv=data_final_collect.map(lambda pair: key_value_to_line(pair))

#	fo=open("/home/cloudera/Amadeus/results_final.csv","w")
	for i in range(len(data_final_collect)):
		pair=data_final_collect[i]
		print("pair=",pair)
		print("line_pair=",key_value_to_line(pair))

	data_final_csv.saveAsTextFile("hdfs://hdfs-host:8020/user/training/data_final.csv")
	input("pause")
