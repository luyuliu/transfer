data = read.csv("C:\\Users\\liu.6544\\Documents\\GitHub\\transfer\\doc\\test_doc\\football_test.csv")


library(lattice)

ks.test(data$ATTP_football_gtfs, data$ATTP_all_gtfs)
ks.test(data$ATTP_football_apc, data$ATTP_all_apc)

ks.test(data$TR_football_gtfs, data$TR_all_gtfs)
ks.test(data$TR_football_apc, data$TR_all_apc)

#wilcox.test()

