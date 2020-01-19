data = read.csv("I:\\OSU\\academic\\transfer_risk\\Transfer\\doc\\rain_test.csv")


library(lattice)

ks.test(data$ATTP_rain_gtfs, data$ATTP_all_gtfs)
ks.test(data$ATTP_rain_apc, data$ATTP_all_apc)

ks.test(data$TR_rain_gtfs, data$TR_all_gtfs)
ks.test(data$TR_rain_apc, data$TR_all_apc)

#wilcox.test()

