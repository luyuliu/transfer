data = read.csv("I:\\OSU\\academic\\transfer_risk\\Transfer\\doc\\nor_dbl.csv")


library(lattice)

ks.test(data$ave_ATTP_normal, data$ave_ATTP_dbl)

ks.test(data$ave_tr_normal, data$ave_tr_dbl)

# wilcox.test()

