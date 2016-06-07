import sys
from pyspark import SparkContext

def get_arrival_airport(line):
	line_split=line.split("^")
	airport=line_split[12]
	return airport.split(" ")[0]

def get_pax(line):
	line_split=line.split("^")
	if len(line_split)<35:
		print("len(line_split) = ",len(line_split))
		print("line split = ",line_split)
		return int(line_split[len(line_split)-4])
	return int(line_split[34])

def get_month(line):
	line_split=line.split("^")
	if len(line_split)<35:
		print("len(line_split) = ",len(line_split))
		print("line split = ",line_split)
		return int(line_split[len(line_split)-2])
	return int(line_split[36])

def is_valid_line(line):
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

def is_valid_airport(line,airports):
	line_split=line.split("^")
	if len(line_split)>=35:
		if line_split[12].split(" ")[0] in airports:
			return True
		else:
			return False
	else:
		if line_split[len(line_split)-4].split(" ")[0] in airports:
			return True
		else:
			return False
    


if __name__=="__main__":
	if len(sys.argv) < 2:
		print >> sys.stderr, "Usage: Exo2 <file>"
		exit(-1)
        
	sc=SparkContext()
    
	valid_airports=['AGP','BCN','MAD']

	data=sc.textFile(sys.argv[1])

	data_filtered=data.filter(lambda line: is_valid_line(line))

	valid_data_filtered=data_filtered.filter(lambda line: is_valid_airport(line,valid_airports))

	airport_pax_month=valid_data_filtered.map(lambda line: (get_arrival_airport(line)+"^"+str(get_month(line)),get_pax(line)))

	total_airport_pax_month=airport_pax_month.reduceByKey(lambda v1,v2:v1+v2)

	sorted_airports=total_airport_pax_month.sortByKey(ascending=False).collect()

	for i_airport in range(len(sorted_airports)):
		airport=sorted_airports[i_airport]
		print("Airport "+str(i_airport+1)+" : "+airport[0]+" with "+str(airport[1])+" passengers")
	input("pause")
