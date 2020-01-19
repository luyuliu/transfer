data = read.csv("C:\\Users\\liu.6544\\Documents\\GitHub\\transfer\\doc\\headway_hour.csv")

cor.test(data$ATTP_gtfs, data$Frequency)
cor.test(data$TR_gtfs, data$Frequency)
cor.test(data$ATTP_apc, data$Frequency)
cor.test(data$TR_apc, data$Frequency)

library("ggpubr")

ggscatter(data, x = "Frequency", y = "ATTP_gtfs", 
          add = "reg.line", conf.int = TRUE, cor.coef.size = 7, font.x = 18, font.y = 20, 
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "ATTP for original GTFS (minutes)")

ggscatter(data, x = "Frequency", y = "ATTP_apc", 
          add = "reg.line", conf.int = TRUE, cor.coef.size = 7, font.x = 18, font.y = 20, 
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "ATTP for APC-GTFS (minutes)")


ggscatter(data, x = "Frequency", y = "TR_gtfs", 
          add = "reg.line", conf.int = TRUE, cor.coef.size = 7, font.x = 18, font.y = 20, 
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "TR for original GTFS (%)")

ggscatter(data, x = "Frequency", y = "TR_apc", 
          add = "reg.line", conf.int = TRUE, cor.coef.size = 7, font.x = 18, font.y = 20, 
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "TR for APC-GTFS (%)")


