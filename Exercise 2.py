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
    


if __name__=="__main__":
	if len(sys.argv) < 2:
		print >> sys.stderr, "Usage: Exo2 <file>"
		exit(-1)
        
	sc=SparkContext()
    
	data=sc.textFile(sys.argv[1])

	data_filtered=data.filter(lambda line: is_valid_line(line))

	airport_pax=data_filtered.map(lambda line: (get_arrival_airport(line),get_pax(line)))

	total_airport_pax=airport_pax.reduceByKey(lambda v1,v2:v1+v2)

	sorted_airports=total_airport_pax.map(lambda data:(data[1],data[0])).sortByKey(ascending=True)

	top10_airports=sorted_airports.top(10)

	for i_airport in range(10):
		airport=top10_airports[i_airport]
		print("Airport "+str(i_airport+1)+" : "+airport[1]+" with "+str(airport[0])+" passengers")
	input("pause")
