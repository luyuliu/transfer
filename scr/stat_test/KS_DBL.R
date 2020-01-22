dbl_data = read.csv("C:\\Users\\liu.6544\\Documents\\GitHub\\transfer\\doc\\nor_dbl_gtfs.csv")

library(lattice)

ks.test(dbl_data$ave_ATTP_normal, dbl_data$ave_ATTP_dbl)
ks.test(dbl_data$ave_tr_normal, dbl_data$ave_tr_dbl)

dbl_data = read.csv("C:\\Users\\liu.6544\\Documents\\GitHub\\transfer\\doc\\nor_dbl_apc.csv")

library(lattice)

ks.test(dbl_data$ave_ATTP_normal, dbl_data$ave_ATTP_dbl)
ks.test(dbl_data$ave_tr_normal, dbl_data$ave_tr_dbl)


#wilcox.test()

