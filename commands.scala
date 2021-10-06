
val inputFile = sc.textFile("PR_Data-1copy.txt")
val pairs = inputFile.map{line => val parts = line.split("\t");(parts(0),parts(1));}
val ranks = pairs.map{case(page,outlinks) => (page,1.0);}
val rightSideRank = pairs.flatMap{ case(page, outlinks) => val links = outlinks.split(","); links.map(link => (link,BigDecimal(1.0/outlinks.split(",").size).setScale(1,BigDecimal.RoundingMode.HALF_UP).toDouble));}
val allRanks = rightSideRank .++ (ranks)
val finalRanks = allRanks.reduceByKey(_+_)
val finalRanksD = finalRanks.map{case(page, rank) => (page, BigDecimal(0.15 + 0.85 * rank).setScale(1,BigDecimal.RoundingMode.HALF_UP).toDouble) ; }


val outlinks = pairs.map{ case(page, outlinks) => (page,outlinks.split(",").size * 1.0);}
val tab = inputFile.flatMap{line => line.split("\t")}
val space = tab.flatMap{line => line.split(",")}
val inlinks = space.map{page => (page,1.0)}
val inlinksTotal = inlinks.reduceByKey(_+_)
val inlinksReal = inlinksTotal.map{case(page,num) => (page, num-1);} 


val allDataPrep = finalRanksD.join(outlinks.join(inlinksReal))
val finale = allDataPrep.map{case(page, rankLinks ) =>  (page, rankLinks._1,rankLinks._2);}
val finale2 = finale.map{case(page,rank,links) => (page,rank, links._1, links._2);}
val sorted = finale2.sortBy(-_._2)
val filtered = sorted.take(500)
System.out.println(filtered.mkString("\n"))




