football_data = read.csv("I:\\OSU\\academic\\transfer_risk\\Transfer\\doc\\football_test.csv")

library(lattice)

ks.test(football_data$ATTP_football_gtfs, football_data$ATTP_all_gtfs)
ks.test(football_data$ATTP_football_apc, football_data$ATTP_all_apc)

ks.test(football_data$TR_football_gtfs, football_data$TR_all_gtfs)
ks.test(football_data$TR_football_apc, football_data$TR_all_apc)

#wilcox.test()

