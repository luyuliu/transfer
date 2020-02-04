data = read.csv("C:\\Users\\liu.6544\\Documents\\GitHub\\transfer\\doc\\test_doc\\headway_hour.csv")

cor.test(data$ATTP_gtfs, data$Frequency)
cor.test(data$ATTP_apc, data$Frequency)
cor.test(data$TR_gtfs, data$Frequency)
cor.test(data$TR_apc, data$Frequency)

library("ggpubr")


ggscatter(data, x = "Frequency", y = "ATTP_gtfs", 
          add = "reg.line", conf.int = TRUE, cor.coef.size = 7, font.x = 18, font.y = 20, 
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "ATTP for original GTFS (minutes)")

attp_apc = ggscatter(data, x = "Frequency", y = "ATTP_apc", size = 2.5,
          add = "reg.line", conf.int = TRUE, cor.coef.size = 5, font.x = 12, font.y = 12, x.text = 15, font.tickslab = c(12),
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "ATTP for APC-GTFS (minutes)")

ggexport(attp_apc, plotlist = NULL, filename = 'C:/Users/liu.6544/Documents/GitHub/transfer/doc/corr_pics/attp_apc.pdf', ncol = NULL,
         nrow = NULL, width = 32, height = 32, pointsize = 12,
         res = NA, verbose = TRUE)


ggscatter(data, x = "Frequency", y = "TR_gtfs", 
          add = "reg.line", conf.int = TRUE, cor.coef.size = 7, font.x = 18, font.y = 20, 
          cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "TR for original GTFS (%)")

tr_apc = ggscatter(data, x = "Frequency", y = "TR_apc", size = 2.5,
                   add = "reg.line", conf.int = TRUE, cor.coef.size = 5, font.x = 12, font.y = 12, x.text = 15, font.tickslab = c(12),
                   cor.coef = TRUE, cor.method = "pearson",
          xlab = "Frequency (hour¯¹)", ylab = "TR for APC-GTFS (%)")

ggexport( tr_apc, plotlist = NULL, filename = 'C:/Users/liu.6544/Documents/GitHub/transfer/doc/corr_pics/tr_apc.eps', ncol = NULL,
         nrow = NULL, width = 3200, height = 3200, pointsize = 12,
         res = 800, verbose = TRUE, device="cairo_ps")


library(ggplot2)

p_tr = ggplot(data, aes(x=Frequency, y=TR_apc)) +
  geom_point(shape= 1, size = 4) +    # Use hollow circles
  geom_smooth(method=lm) +
  xlim(NA, 2.49) +
  theme(text = element_text(size=18),
        axis.text.x = element_text(angle = 0))
p_tr = p_tr + labs(x = "Frequency (1/hour)", y = "Transfer risk for APC-GTFS (%)")
p_tr
ggsave("C:/Users/liu.6544/Documents/GitHub/transfer/doc/corr_pics/tr_apc.svg")

p_tr = ggplot(data, aes(x=Frequency, y=ATTP_apc)) +
  geom_point(shape= 1, size = 4) +    # Use hollow circles
  geom_smooth(method=lm) +
  xlim(NA, 2.49) +
  theme(text = element_text(size=18),
        axis.text.x = element_text(angle = 0))
p_tr = p_tr + labs(x = "Frequency (1/hour)", y = "Average total time penalty for APC-GTFS (minutes)")
p_tr
ggsave("C:/Users/liu.6544/Documents/GitHub/transfer/doc/corr_pics/attp_apc.svg")

