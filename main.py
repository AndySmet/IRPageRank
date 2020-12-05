from pyspark import SparkContext, StorageLevel
from operator import add, sub
import re


def computeRankScore(outlinks, PR, di):
    totalOutlinks = len(outlinks)

    # yield is de emit in de slides, das eigenlijk een return die de toestand van de functie bijhoudt en de volgende keer start bij yield in plaats van het begin van de functie
    for link in outlinks:
        yield (link, PR / totalOutlinks)
    yield (di, outlinks)


# functie die word gebruikt als we iets gaan doen met sink nodes word nu nog niet echt gebruikt
def addifin(list, nummer):
    # if nummer in list:
    return list
    # else:
    return list + [nummer]


sc = SparkContext("local", "first app")

# logFile="ClueWeb09_WG_50m.graph-txt"
# logData=sc.textFile(logFile).zipWithIndex()


# maak een RDD met values 0 .... en indexen die de edges weergeven van het voorbeeld in de slides (yahoo amazon en microsoft)
rd = sc.parallelize([[0, 1], [0, 2], [1]]).zipWithIndex()
# switch de values en keys zodat de keys de page zelf is en de values de edges
rd = rd.map(lambda x: (x[1], addifin(x[0], x[1])))
# krijg de N voor de formules
n = rd.count()

print(n)
# initializeer alle rank scores op 1/n
ranks = rd.mapValues(lambda x: 1.0 / n)
# meer prints voor duidelijkheid van wat er is aant gebeuren
print(rd.collect())
print(ranks.collect())
print(rd.join(ranks).collect())
# initializeer waardes die gebruikt worden in formules b = teleportwaarde Beta
b = 0.85
epsilon = 0.00001

# initializeer val > 1 en iteratie count op 0 en previousrank ri op de 1/n rank scores
val = 2
iter = 0
ri = ranks

# blijf loopen zolang de epsilon waarde niet is bereikt
# epsilon is het verschil in rankscores nu en vorige iteratie
# er zijn 3 normen die hier staan N0= maximum fout  N1= som van absolute fouten N2= wortel van alle fouten **2
while val > epsilon:
    iter += 1

    # join rd en ranks zodat de links ranks en scores samen in een RDD zitten en mappen de scores in een nieuwe RDD
    contribs = rd.join(ranks).flatMap(
        lambda url_ranks: computeRankScore(url_ranks[1][0], url_ranks[1][1], url_ranks[0]))
    print(contribs.collect())
    # reducebykey(add) telt alles values die dezelfde keys hebben bij elkaar op en de map values past de formule van random teleports toe op deze score en update de nieuwe ranksscores
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * b + (1 - b) * 1 / n)
    # bereken alle normen die nodig zijn met reduce en map functies.
    diff = ranks.join(ri).mapValues(lambda rank: abs(rank[0] - rank[1]))
    diffsquared = diff.mapValues(lambda x: x ** 2)
    val = diff.values().reduce(add)
    print("NORM N1:", val)
    print("NORM N2:", diffsquared.values().reduce(add) ** 0.5)
    print("NORM N0:", diff.values().reduce(lambda x, y: max(x, y)))
    # sla de huidige ranks op in de oude ri.
    ri = ranks
print(iter)