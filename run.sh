#!/bin/sh

hadoop fs -rm -r enwiki.invertedindex

hadoop jar wikipedia-1.0.jar seis736.wikipedia.WikipediaDriver -libjars /home/shuo/Cloud9/dist/cloud9-1.4.17.jar,/home/shuo/Cloud9/lib/guava-14.0-rc3.jar,/home/shuo/Cloud9/lib/fastutil-6.5.4.jar,/home/shuo/Cloud9/lib/bliki-core-3.0.16.jar

hadoop fs -libjars wikipedia-1.0.jar -text enwiki.invertedindex/part-r-00000.snappy | head
