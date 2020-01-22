dbl_data = read.csv("C:\\Users\\liu.6544\\Documents\\GitHub\\transfer\\doc\\test_doc\\a&b.csv")

library(lattice)

ks.test(dbl_data$a_ATTP, dbl_data$b_ATTP)
ks.test(dbl_data$a_tr, dbl_data$b_tr)
