import sys
from pyspark import SparkContext

if __name__=="__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: Exo1 <file>"
        exit(-1)
        
    sc=SparkContext()

    files=sys.argv[1].split(',')

    bookings=files[0]
    searches=files[1]

    data_bookings=sc.textFile(bookings)
    data_searches=sc.textFile(searches)

    N_bookings=data_bookings.count()
    N_searches=data_searches.count()
    
    print("Nombre de lignes bookings.csv= ",N_bookings)
    print("Nombre de lignes searches.csv= ",N_searches)
    input("pause")
