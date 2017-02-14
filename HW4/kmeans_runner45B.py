#!/usr/bin/env python
#START STUDENT CODE45_RUNNER
import numpy as np
import sys
from Kmeans import MRKmeans, stop_criterion

# set the randomizer seed so results are the same each time.
np.random.seed(0)

# define mrjob runner
mr_job = MRKmeans(args=["topUsers_Apr-Jul_2014_1000-words.txt", '--file=Centroids.txt'])


centroid_points = []
k = 2
class_codes = {'0.0':'Human', '1.0':'Cyborg', '2.0':'Robot', '3.0':'Spammer'}

def startCentroidsBC(k):
    import re
    counter = 0
    for line in open("topUsers_Apr-Jul_2014_1000-words_summaries.txt").readlines():
        if counter == 1:        
            data = re.split(",",line)
            globalAggregate = [float(data[i+3])/float(data[2]) for i in range(1000)]
        counter += 1
    #perturb the global aggregate for the four initializations    
    centroids = []
    for i in range(k):
        rndpoints = np.random.sample(1000)
        peturpoints = [rndpoints[n]/10+globalAggregate[n] for n in range(1000)]
        centroids.append(peturpoints)
        total = 0
        for j in range(len(centroids[i])):
            total += centroids[i][j]
        for j in range(len(centroids[i])):
            centroids[i][j] = centroids[i][j]/total
    return centroids

# write initial centroids to file
centroid_points = startCentroidsBC(k)
with open('Centroids.txt', 'w+') as f:
    f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)
f.close()

# Update centroids iteratively
i = 0
while(1):
    # save previous centoids to check convergency
    centroid_points_old = centroid_points[:]
    print "iteration"+str(i)+":"
    with mr_job.make_runner() as runner: 
        runner.run()
        centroid_points = []
        clusters = {}
        # stream_output: get access of the output 
        for line in runner.stream_output():
            key,value =  mr_job.parse_output_line(line)
            centroid, codes = value
            centroid_points.append(centroid)
            clusters[key] = codes
            
    # Update the centroids for the next iteration
    with open('Centroids.txt', 'w') as f:
        f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)
        
    print "\n"
    i = i + 1
    max_class={}
    if(stop_criterion(centroid_points_old,centroid_points,0.01)):
        print "Centroids\n"
        print centroid_points
        print "\n\n\n"
        print "Breakdown by class code:"
        for cluster_id, cluster in clusters.iteritems():
            max_class[cluster_id] = max(cluster.values())
            print "Cluster ID:", cluster_id
            print "Human:", cluster.get('0',0)
            print "Cyborg:", cluster.get('1',0)
            print "Robot:", cluster.get('2',0)
            print "Spammer:", cluster.get('3',0)
            print "\n"
        print "purity = ", sum(max_class.values())/1000.0*100
        break