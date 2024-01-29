rdd = sc.textFile("Complete_Shakespeare.txt")
rdd.count()
words = rdd.flatMap(lambda line: line.split(" "))
words.distinct().count()
word_pairs = words.map(lambda word: (word, 1))
word_count = word_pairs.reduceByKey(lambda a, b : a + b)
word_count.top(10, lambda x: x[1])
word_asc = word_count.sortBy(lambda x: -x[1]) - this does not work because it performs lazy eval?
word_asc = word_count.sortBy(lambda x: -x[1]).collect() - this works because its action
then you can do word_asc[:10]
