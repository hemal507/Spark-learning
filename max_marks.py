from pyspark import SparkContext
import sys

if __name__ == "__main__" :
	if len(sys.argv) != 2 :
		print("Usage - pass marks file path as argument")
		exit(-1)
	else :
		marks_file = sys.argv[1]
		sc  = SparkContext()
		marks_rdd = sc.textFile(marks_file)

		zero_op  = ('',0)
		seq_op   = (lambda local_max , element : local_max if local_max[1] > element[1] else element)
		comb_op  = (lambda comb_max1 , comb_max2 : comb_max1 if comb_max1[1] > comb_max2[1] else comb_max2)
		max_marks = marks_rdd.map(lambda record : record.split(","))					\
					.map(lambda tuple1 : (tuple1[0] , (tuple1[1], int(tuple1[2]) )))	\
					.groupByKey()								\
					.map(lambda tuple3 : (tuple3[0] , max(x[1] for x in tuple3[1] ) ))
				
		for student in max_marks.collect() :
			print(student[0] , student[1] )
		
'''
AGGREGATE_BY_KEY
		zero_op   = (0)
		seq_op    = (lambda local_max , element : local_max if local_max > element[1] else element[1])
		comb_op   = (lambda local_max1 , local_max2 : local_max1 if local_max1 > local_max2 else local_max2)
		max_marks = marks_rdd.map(lambda record : record.split(","))					\
					.map(lambda tuple1 : (tuple1[0] , (tuple1[1] , int(tuple1[2]))))	\
					.aggregateByKey(zero_op , seq_op , comb_op)			

COMBINE_BY_KEY
		zero_op  = (lambda tuple1 : (tuple1[0],tuple1[1]))
		seq_op   = (lambda local_max , element : local_max if local_max[1] > element[1] else element)
		comb_op  = (lambda comb_max1 , comb_max2 : comb_max1 if comb_max1[1] > comb_max2[1] else comb_max2)
		max_marks = marks_rdd.map(lambda record : record.split(","))					\
					.map(lambda tuple1 : (tuple1[0] , (tuple1[1], int(tuple1[2]) )))	\
					.combineByKey(zero_op , seq_op , comb_op)

REDUCE_BY_KEY
		comb_op  = (lambda comb_max1 , comb_max2 : comb_max1 if comb_max1[1] > comb_max2[1] else comb_max2)
		max_marks = marks_rdd.map(lambda record : record.split(","))					\
					.map(lambda tuple1 : (tuple1[0] , (tuple1[1], int(tuple1[2]) )))	\
					.reduceByKey(comb_op)

GROUP_BY_KEY - just grouping
		max_marks = marks_rdd.map(lambda record : record.split(","))					\
					.map(lambda tuple1 : (tuple1[0] , (tuple1[1], int(tuple1[2]) )))	\
					.groupByKey()								
		for student in max_marks.collect() :
			print(student[0] , [x for x in student[1]] )

GROUP_BY_KEY - mappings after groupby
		max_marks = marks_rdd.map(lambda record : record.split(","))					\
					.map(lambda tuple1 : (tuple1[0] , (tuple1[1], int(tuple1[2]) )))	\
					.groupByKey()								\
					.map(lambda tuple3 : (tuple3[0] , max(x[1] for x in tuple3[1] ) ))
				
		for student in max_marks.collect() :
			print(student[0] , student[1] )


'''

