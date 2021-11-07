#! /bin/bash

for i in {1..10}
do
	for j in {1..10}
	do
		for k in {1..100}
		do
			mkdir -p testing/$i/$j testing/pass
			touch testing/$i/$j/$k
		done
	done
done
