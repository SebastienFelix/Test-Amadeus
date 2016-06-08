import sys
from pyspark import SparkContext

import plotly
plotly.tools.set_credentials_file(username='DemoAccount', api_key='lr1c37zw81')
import plotly.graph_objs as go

import numpy as np

def get_arrival_airport(line):
	line_split=line.split("^")
	if len(line_split)<35:
		airport=line_split[10]
		return airport.split(" ")[0]
	airport=line_split[12]
	return airport.split(" ")[0]

def get_pax(line):
	line_split=line.split("^")
	if len(line_split)<35:
		return int(line_split[len(line_split)-4])
	return int(line_split[34])

def get_month(line):
	line_split=line.split("^")
	if len(line_split)<35:
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
		print >> sys.stderr, "Usage: Exercise 3 <file>"
		exit(-1)
        
	sc=SparkContext()
    
	valid_airports=['AGP','BCN','MAD']

	data=sc.textFile(sys.argv[1])

	data_filtered=data.filter(lambda line: is_valid_line(line))

	valid_data_filtered=data_filtered.filter(lambda line: is_valid_airport(line,valid_airports))

	airport_pax_month=valid_data_filtered.map(lambda line: (get_arrival_airport(line)+"^"+str(get_month(line)),get_pax(line)))

	total_airport_pax_month=airport_pax_month.reduceByKey(lambda v1,v2:v1+v2)

	sorted_airports=total_airport_pax_month.sortByKey(ascending=False).collect()

	coordinate_x=np.linspace(1,12,12)
	coordinate_y_AGP=np.linspace(1,12,12)
	coordinate_y_BCN=np.linspace(1,12,12)
	coordinate_y_MAD=np.linspace(1,12,12)


	for i_airport in range(len(sorted_airports)):
		airport=sorted_airports[i_airport]
		airport_split_0=airport[0].split("^")[0]
		airport_split_1=airport[0].split("^")[1]
		if airport_split_0=='AGP':
			coordinate_y_AGP[int(airport_split_1)-1]=airport[1]
		if airport_split_0=='BCN':
			coordinate_y_BCN[int(airport_split_1)-1]=airport[1]
		if airport_split_0=='MAD':
			coordinate_y_MAD[int(airport_split_1)-1]=airport[1]
		print("Airport "+str(i_airport+1)+" : "+airport[0]+" with "+str(airport[1])+" passengers")
	print("AGP : ",coordinate_y_AGP)
	print("BCN : ",coordinate_y_BCN)
	print("MAD : ",coordinate_y_MAD)
	trace_AGP=go.Scatter(x=coordinate_x,y=coordinate_y_AGP,mode='line+markers',name='AGP')
	trace_BCN=go.Scatter(x=coordinate_x,y=coordinate_y_BCN,mode='line+markers',name='BCN')
	trace_MAD=go.Scatter(x=coordinate_x,y=coordinate_y_MAD,mode='line+markers',name='MAD')

	traces=[trace_AGP,trace_BCN,trace_MAD]

	url=plotly.plotly.plot(traces,filename='airports_monthes')
	print("plot_URL : ",url)
	input("pause")
