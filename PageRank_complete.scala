/*
Programmer: Nikola Andric
Last Eddited: 10/06/2021

Description: This program is using spark to calculate pageRank value of each given page from the input,
		as well as inlinks and outlinks values.
		
		PR(Tn) - Each page has its own self-importance (rank). PR of each page depends on the PR of pages pointing to it.
		C(Tn) - Each page spreads its rank evenly amongst all of its outgoing links. The number of outgoing links for page n is C(Tn).
		PR(Tn)/C(Tn) - The share of the rank that page A will get from the page n.
		d - In order to stop the other pages of having too much influence, the total rank is damped down by multiplying it by 0.85 (factor d)000
		(1 - d) - If a page has no lniks to it even then it will get a smapp PR of 0.15.
		
*/

/*
PART_1: Calculate pageRank value of each webpage using the formula PR(T0) = (1-d) + d [PR(T1)/C(T1) + PR(T2)/C(T2) + .... PR(Tn)/C(Tn) ] using d = 0.85.

PART_2: Calculate number of inlinks and outlinks of each webpage.

PART_3: Show top 500 page-ranked webpages in descending order. The output shoulb look like : page pageRank outlinks inlinks.

*/

//*************************************************************************  PART_1  *************************************************************************
//Reading in the input file as a variable (creating the first RDD)
//sc -> Spark Context
val inputFile = sc.textFile("PR_Data-1copy.txt")

//Separate line into 2 parts
val pairs = inputFile.map{line => val parts = line.split("\t");(parts(0),parts(1));}

// New map with pages from the left side of the line and hardcoded pageRank value of 1.0.
val ranks = pairs.map{case(page,outlinks) => (page,1.0);}

//Calculate ranks of each of the pages on the right side by number of pages on hte right side of the line (ex. a   c,d,e =>    c = 1/3 d = 1/3 3 = 1/3)
val rightSideRank = pairs.flatMap{ case(page, outlinks) => val links = outlinks.split(","); links.map(link => (link,BigDecimal(1.0/outlinks.split(",").size).setScale(1,BigDecimal.RoundingMode.HALF_UP).toDouble));}

//Join the two RDDs into one
val allRanks = rightSideRank .++ (ranks)

//Reduce by key
val finalRanks = allRanks.reduceByKey(_+_)

//Finally, eneter the rest of the equation for pageRank value.
val finalRanksD = finalRanks.map{case(page, rank) => (page, BigDecimal(0.15 + 0.85 * rank).setScale(1,BigDecimal.RoundingMode.HALF_UP).toDouble) ; }

//*************************************************************************  PART_2  *************************************************************************

// Get number of outlinks of each page by using size of the right side of the pairs. Myltiple by 1.0 to convert to Double.
val outlinks = pairs.map{ case(page, outlinks) => (page,outlinks.split(",").size * 1.0);}

// Get all inlinks by flat mapping every single page instance from input file, then
// assign each instance with 1.0, then
// reduce by key and subtract 1 for all instances from the left side of the input file.
val tab = inputFile.flatMap{line => line.split("\t")}
val space = tab.flatMap{line => line.split(",")}
val inlinks = space.map{page => (page,1.0)}
val inlinksTotal = inlinks.reduceByKey(_+_)
val inlinksReal = inlinksTotal.map{case(page,num) => (page, num-1);} 

//*************************************************************************  PART_3  *************************************************************************

// Join all maps into one. Form: (page,(rank,(outlinks,inlinks))).
val allDataPrep = finalRanksD.join(outlinks.join(inlinksReal))

// Create new RDD in form (page,(rank,outlinks,inlinks)).
val finale = allDataPrep.map{case(page, rankLinks ) =>  (page, rankLinks._1,rankLinks._2);}

// Create new RDD in form (page,rank,outlinks,inlinks).
val finale2 = finale.map{case(page,rank,links) => (page,rank, links._1, links._2);}

// Sort them in reverse order.
val sorted = finale2.sortBy(-_._2)

// Show top 500 pages from the list.
val filtered = sorted.take(500)
System.out.println(filtered.mkString("\n"))











val allDataPrep = finalRanksD .++ (outlinks) 
val allDataPrep2 = allDataPrep .++ (inlinksReal)

val sorted = allDataPrep2.sortBy(-_._2)

//val linksTogether = allDataPrep.map{case(page,inAndOut) => val together = inAndOut._1.join(inAndOut._2); (page, together); }
//val allDataPrep = linksTogether.join(finalRanksD.join(linksTogether))
//val allData = allDataPrep.map{case(page,rank) => (page, rank._1, rank._2, rank._3)}




