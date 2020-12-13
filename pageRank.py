from pyspark import SparkContext
import time
import os
import shutil
from operator import add


def computeRankScore(outlinks, PR, sourceLink):

    # yield is de emit in de slides, das eigenlijk een return die de toestand van de functie bijhoudt en de volgende keer start bij yield in plaats van het begin van de functie
    for link in outlinks:
        yield (link, PR / len(outlinks))

    yield (sourceLink, 0)

# functie die word gebruikt als we iets gaan doen met sink nodes word nu nog niet echt gebruikt
def addifin(list, nummer):
    if nummer in list:
        return list
    else:
        return list + [nummer]


#start the pageranking algorithm with the values in order
#filename, we support web-Google.txt and ClueWeb09_WG format
#beta, the probabibility to not teleport
#epsilon, value for convergence calculations
#allnorms, True means 3 norms will be calculated and a timetable will be generated, False means only Ninf norm will be used and no timetable will be generated
#commonsolution,True means using the backlink method to fix sinknodes, false means giving every node a link to itself, if it doesn't have one already

def pageRank(filename,beta,epsilon,allnorms=False,commonsolution=False):
    #initialize the RDDs in the correct formats, (ID, OUTGOINGLINKS)
    #initial maps en group om de data in juist formaat te krijgen (id , lijst van outgoing links)
    if commonsolution:
        #common solution is making backlinks for all sink nodes
        if filename=="web-Google.txt":
            rd = sc.textFile(filename)
            rd = rd.map(lambda x: (int(x.split("\t")[0]),int(x.split("\t")[1])))
            rd = rd.groupByKey().cache()
            rd = rd.mapValues(lambda x:list(x))
        else:
            rd = sc.textFile(filename).zipWithIndex()
            rd = rd.map(
                lambda x: (x[1], list(filter(lambda z: z < 50e6, [int(z) for z in x[0].split()])))).filter(
                lambda x: x[0] < 50e6)

        #initialize incoming links for sink nodes
        m2=rd.flatMapValues(lambda x:x).map(lambda x:(x[1],x[0])).groupByKey()
        m2=m2.mapValues(lambda x:list(x))

        # maak backlinks voor alle sink nodes
        rd3=rd.fullOuterJoin(m2)
        rd3=rd3.filter(lambda x: x[1][0]==None).map(lambda x: (x[0],x[1][1]))

        rd=rd.union(rd3)
    else:
        # testsolution is making spidertraps out of all sink nodes
        if filename=="web-Google.txt":
            rd = sc.textFile(filename)
            rd = rd.map(lambda x: (int(x.split("\t")[0]),int(x.split("\t")[1])))
            values=rd.values().distinct().map(lambda x:(x,x))
            rd = rd.union(values).distinct()
            rd = rd.groupByKey()
            rd = rd.mapValues(lambda x: (list(x))).cache()
        else:
            rd = sc.textFile(filename).zipWithIndex()
            rd = rd.map(lambda x: (
            x[1], list(filter(lambda z: z < 50e6, addifin([int(z) for z in x[0].split()], x[1]))))).filter(
                lambda x: x[0] < 50e6)
    # get N used in formulas
    n=rd.count()
    print(n)
    # initialize all ranks on 1/n
    ranks = rd.mapValues(lambda x: 1.0 / n)
    # initialize value above 1, iterations at 0 and the previous ranks ri to the ranks we just initialized
    val = 2
    iter = 0
    ri = ranks

    #keep looping aslong as the difference between the former ranks and the current ranks is bigger than epsilon
    #bigger means that the norm of the residual vector abs(ri-ranks).
    # we test for 3 norms, Ninf= max(vector) N1 = sum(vector) N2= root of sum of all elements squared
    #time table and timer variables are initialized aswell
    start_time=time.time()
    timetable={}
    while val > epsilon:

        iter += 1
        # join rd and ranks to get links together with scores in the same RDD and use flatmap to calculate all contributions
        # to the pagerank score with our generator function
        # the map and reduce function are based on https://www.coursera.org/lecture/big-data-analysis/rdd-implementation-F3Y0c
        # this is a course on efficient pageranking with RDDs and is
        # the operations match the map and reduce operations given in the lectures very well, so we used it.
        startContribs=time.time()
        contribs = rd.join(ranks).flatMap(
            lambda url_ranks: computeRankScore(url_ranks[1][0], url_ranks[1][1], url_ranks[0]))

        # contribs now contains a list of tuples in the form of (ID, score) reducebykey will add all these values, this is the summation in the formula
        # the mapvalues will use adjust the rank with the formula rank= rank*b + a/n

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * beta + (1 - beta) * 1.0 / n)
        # calculate the norms we will use and print the results.
        diff = ranks.join(ri).mapValues(lambda rank: abs(rank[0] - rank[1]))

        ninf = diff.values().max()

        val = ninf
        if allnorms:
            diffsquared=diff.mapValues(lambda rank:rank**2)
            n1=diff.values().sum()
            n2=diffsquared.values().reduce(add) ** 0.5
            timetable[iter]=[ninf,n1,n2]
            print("NORM N1:", n1)
            print("NORM Ninf:", ninf)
            print("NORM N2:", n2)
        else:
            print("NORM Ninf:", ninf)

        # set old ranks equal to the calculated ranks and print the time it took for current iteration
        ri = ranks
        time2=time.time()
        print("iteration {} duurt ".format(iter),time2-start_time)
        start_time=time2

    print("iteration {} zorgt voor een epsilon van {}".format(iter,epsilon))
    #sort the values with a coalesce zo there is only one partition and one file
    lines=ranks.coalesce(1,False).sortBy(lambda a:a[1],False,1)
    # coalesce once more and write to ranks.csv, this generates a directory instead of a file so we will move the file later.
    lines.coalesce(1,False).saveAsTextFile("ranks.csv")
    # generate a timetable if comparing all norms
    if allnorms:
        stringske="timetable : \n"
        stringratios="ratios :\n"
        t=[1,1,1]
        for i in timetable:
            string2="time {} {} \n".format(i,timetable[i])
            stringratios+="time {} {}\n".format(i,[timetable[i][0]/t[0],timetable[i][1]/t[1],timetable[i][2]/t[2]])
            t=timetable[i]
            stringske+=string2

        a=open("timetable.txt","w")
        r=open("ratios.txt","w")
        a.write(stringske)
        r.write(stringratios)
        a.close()
        r.close()

if __name__ == "__main__":
    # remove all files that will be generated again
    try:
        shutil.rmtree('ranks.csv')
    except:
        pass
    try:
        os.remove('pagerank_list.csv')


    except:
        pass
    # start sparkcontext with all cores and print weburl where u can monitor spark and start a timer for the program.
    sc = SparkContext("local[*]", "first app")
    print(sc.uiWebUrl)
    start_of_program=time.time()
    #start the pageranking algorithm with the values in order
    #filename, we support web-Google.txt and ClueWeb09_WG format
    #beta, the probabibility to not teleport
    #epsilon, value for convergence calculations
    #allnorms, True means 3 norms will be calculated and a timetable will be generated, False means only Ninf norm will be used and no timetable will be generated
    #commonsolution,True means using the backlink method to fix sinknodes, false means giving every node a link to itself, if it doesn't have one already
    pageRank("web-Google.txt",0.85,1e-6,False,False)

    # move and remove the files that are not needed anymore and print the total time it took for the program to run.
    try:
        shutil.move("ranks.csv/part-00000", "pagerank_list.csv")
        try:
            shutil.rmtree('ranks.csv')
        except:
            pass

    except:
        print('U can find it in the ranks.csv directory under part-00000')
    print("total time of program: ", time.time() - start_of_program)
