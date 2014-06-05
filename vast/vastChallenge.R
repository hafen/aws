
# install.packages("devtools")
library(devtools)
install_github("vastChallenge", "hafen", subdir = "package")
library(datadr)
library(cyberTools)
library(Rhipe)
rhinit()

# rhoptions(runner = "/share/apps/R/3.0.2/bin/R CMD /share/apps/R/3.0.2/lib64/R/library/Rhipe/bin/RhipeMapReduce --slave --silent --vanilla")

## Vagrant Ubuntu
# rhoptions(runner = "/usr/lib/R/bin/R CMD /usr/local/lib/R/site-library/Rhipe/bin/RhipeMapReduce --slave --silent --vanilla")
## need to get this setup for the right user
# hadoop fs -mkdir -p /user/rstudio/vastChallenge/data
# hadoop fs -mkdir -p /user/rstudio/vastChallenge/data/nfRaw
# hadoop fs -mkdir -p /user/rstudio/vastChallenge/data/raw/nf
# hadoop fs -put /vagrant/nf-week2.csv /user/rstudio/vastChallenge/data/raw/nf
## _rh_meta was throwing things off!
# hadoop fs -rm -r /vagrant/nf-week2.csv /user/rstudio/vastChallenge/data/raw/nf/_rh_meta

#hdfs.setwd("/user/perk387/vastChallenge/data")
rhmkdir("/user/user3/vastChallenge")

rhmkdir("/user/user3/vastChallenge/data")
rhmkdir("/user/user3/vastChallenge/data/raw")
rhmkdir("/user/user3/vastChallenge/data/raw/nf")
hdfs.setwd("/user/user3/vastChallenge/data")


nfTransform <- function(x) {
  x$date <- as.POSIXct(x$TimeSeconds, origin = "1970-01-01", tz = "UTC")
  x[,setdiff(names(x), c("TimeSeconds", "parsedDate"))]
}


nfRaw <- ddf(hdfsConn("nfRaw"))
## OR ##
csvConn <- hdfsConn("raw/nf")
nfConn <- hdfsConn("nfRaw")
nfRaw <- drRead.csv(csvConn, output = nfConn, postTransFn = nfTransform,overwrite=TRUE)
nfRaw <- updateAttributes(nfRaw)

names(nfRaw)
nrow(nfRaw)
str(nfRaw[[1]])

summary(nfRaw)

# grab the full frequency table for firstSeenSrcIp
srcIpFreq <- summary(nfRaw)$firstSeenSrcIp$freqTable
# look at the top few IPs
head(srcIpFreq)
head(hostListOrig)
srcIpFreq <- mergeHostList(srcIpFreq, "value", original = TRUE)
head(srcIpFreq)



# see how many of each type we have
table(srcIpFreq$type)
# look at 172.x addresses that aren't in our host list
sort(subset(srcIpFreq, type == "Other 172.*")$value)

hostListOrig$IP[grepl("172\\.20\\.1", hostListOrig$IP)]



srcIpFreq <- summary(nfRaw)$firstSeenSrcIp$freqTable
srcIpFreq <- mergeHostList(srcIpFreq, "value")
table(srcIpFreq$type)



# for each type, get the quantiles
srcIpFreqQuant <- groupQuantile(srcIpFreq, "type")
# quantile plot by host type
xyplot(Freq ~ p | type, data = srcIpFreqQuant, 
       layout = c(7, 1), type = c("p", "g"), 
       between = list(x = 0.25), 
       scales = list(y = list(log = 10)),
       xlab = "Sample Fraction",
       ylab = "Number of Connections as Source IP"
)

destIpFreq <- summary(nfRaw)$firstSeenDestIp$freqTable
destIpFreq <- mergeHostList(destIpFreq, "value", original = TRUE)





subset(destIpFreq, type == "Other")



destIpFreq <- summary(nfRaw)$firstSeenDestIp$freqTable
destIpFreq <- mergeHostList(destIpFreq, "value")



subset(destIpFreq, type == "Other 172.*")



destIpFreqQuant <- groupQuantile(destIpFreq, "type")

srcDestIpFreqQuant <- make.groups(source = srcIpFreqQuant, destination = destIpFreqQuant)

xyplot(Freq ~ 100 * p | type, groups = which, 
       data = srcDestIpFreqQuant, 
       layout = c(7, 1), type = c("p", "g"), 
       between = list(x = 0.25), 
       scales = list(y = list(log = 10)),
       xlab = "Percentile",
       ylab = "Number of Connections",
       subset = type != "Other",
       auto.key = TRUE
)

freqMerge <- merge(srcIpFreq, destIpFreq[,c("value", "Freq", "type")], by="value",
                   suffixes = c(".src", ".dest"), all = TRUE)
freqMerge$type <- freqMerge$type.src
freqMerge$type[is.na(freqMerge$type)] <- freqMerge$type.dest[is.na(freqMerge$type)]
freqMerge$Freq.src[is.na(freqMerge$Freq.src)] <- 0
freqMerge$Freq.dest[is.na(freqMerge$Freq.dest)] <- 0

xyplot(log10(Freq.dest + 1) ~ log10(Freq.src + 1) | type, data = freqMerge,
       # scales = list(relation = "free"),
       xlab = "log10 number of times host is first seen source",
       ylab = "log10 number of times host is first seen dest",
       # subset = !type %in% c("SMTP", "Administrator", "Domain controller"),
       type = c("p", "g"),
       panel = function(x, y, ...) {
         panel.xyplot(x, y, ...)
         panel.abline(a = 0, b = 1)
       },
       between = list(x = 0.25, y = 0.25),
       as.table = TRUE,
       aspect = "iso"
)


freqMerge$tot <- freqMerge$Freq.src + freqMerge$Freq.dest
topTot <- head(freqMerge[order(freqMerge$tot, decreasing = TRUE),], 10)
topTot



bigIPs <- topTot$value[1:4]

# aggregate by minute and IP for just "bigIPs"
bigTimeAgg <- drXtabs(~ timeMinute + firstSeenDestIp, data = nfRaw, transFn = function(x) {
  x <- subset(x, firstSeenDestIp %in% bigIPs)
  x$timeMinute <- as.POSIXct(trunc(x$date, 0, units = "mins"))
  x
})
# sort by IP and time
bigTimeAgg <- bigTimeAgg[order(bigTimeAgg$firstSeenDestIp, bigTimeAgg$timeMinute),]
# convert time back to POSIXct
bigTimeAgg$timeMinute <- as.POSIXct(bigTimeAgg$timeMinute, tz = "UTC")
save(bigTimeAgg, file = "data/artifacts/bigTimeAgg.Rdata")

xyplot(Freq ~ timeMinute | firstSeenDestIp, 
       data = bigTimeAgg, 
       layout = c(1, 4), as.table = TRUE, 
       strip = FALSE, strip.left = TRUE, 
       between = list(y = 0.25),
       type = c("p", "g"))



bigTimeAgg[which.max(bigTimeAgg$Freq),]

# retrieve rows from netflow data with highest count
busiest <- drSubset(nfRaw, 
                    (firstSeenDestIp == "172.30.0.4" | firstSeenSrcIp == "172.30.0.4") &
                      trunc(date, 0, units = "mins") == as.POSIXct("2013-04-11 12:55:00", tz = "UTC"))
# order by time
busiest <- busiest[order(busiest$date),]
save(busiest, file = "data/artifacts/busiest.Rdata")



table(busiest$firstSeenSrcIp)



busiest$cumulative <- seq_len(nrow(busiest))
xyplot(cumulative ~ date | firstSeenSrcIp, data = busiest, pch = ".",
       xlab = "Time (seconds)",
       ylab = "Cumulatuve Number of Connections",
       between = list(x = 0.25, y = 0.25),
       layout = c(3, 3),
       type = c("p", "g"),
       strip = FALSE, strip.left = TRUE
)



table(subset(busiest, firstSeenSrcIp == "172.30.0.4")$firstSeenSrcPort)



busiest2 <- busiest[busiest$firstSeenSrcIp != "172.30.0.4",]
table(busiest2$firstSeenDestPort)



table(busiest2$firstSeenSrcPayloadBytes)


bigTimes <- sort(unique(bigTimeAgg$timeMinute[bigTimeAgg$Freq > 1000]))



xyplot(Freq ~ timeMinute | firstSeenDestIp, data = bigTimeAgg,  layout = c(1, 4), 
       strip = FALSE, strip.left = TRUE, 
       as.table = TRUE, 
       between = list(y = 0.25), 
       groups = timeMinute %in% bigTimes)

#load("data/artifacts/bigTimeAgg.Rdata")
bigTimesHostAgg <- drXtabs(~ firstSeenSrcIp, by = "firstSeenDestIp", 
                           data = nfRaw, 
                           transFn = function(x) {
                             x$timeMinute <- as.POSIXct(trunc(x$date, 0, units = "mins"))
                             x <- subset(x, firstSeenDestIp %in% bigIPs & timeMinute %in% bigTimes)
                             x
                           })
save(bigTimesHostAgg, file = "data/artifacts/bigTimesHostAgg.Rdata")



lapply(bigTimesHostAgg, function(x) x[x$Freq > 100000,])



# get all IPs involved in the DDoS
badIPs <- unique(do.call(c, lapply(bigTimesHostAgg, 
                                   function(x) x$firstSeenSrcIp[x$Freq > 100000])))
# do these match with the large values in srcIpFreq for "External"?
head(subset(srcIpFreq, type == "External"), 20)


timeAgg <- drXtabs(~ timeMinute + firstSeenDestIp, data = nfRaw, 
                   transFn = function(x) {
                     x$timeMinute <- as.POSIXct(trunc(x$date, 0, units = "mins"))
                     subset(x, firstSeenDestIp %in% bigIPs &
                              !(timeMinute %in% bigTimes & 
                                  firstSeenSrcIp %in% c(bigIPs, badIPs) & 
                                  firstSeenDestIp %in% c(bigIPs, badIPs)))
                   })
timeAgg <- timeAgg[order(timeAgg$timeMinute),]
timeAgg$timeMinute <- as.POSIXct(timeAgg$timeMinute, tz = "UTC")
save(timeAgg, file = "data/artifacts/timeAgg.Rdata")



xyplot(log10(Freq + 1) ~ timeMinute | firstSeenDestIp, 
       data = timeAgg, 
       layout = c(1, 4), as.table = TRUE, 
       strip = FALSE, strip.left = TRUE, 
       between = list(y = 0.25),
       type = c("p", "g"))

data.frame(xtabs(~ Species, data = iris))



srcIpByte <- drXtabs(firstSeenSrcPayloadBytes ~ firstSeenSrcIp, 
                     data = nfRaw)
# merge in hostList
srcIpByte <- mergeHostList(srcIpByte, "firstSeenSrcIp")
save(srcIpByte, file = "data/artifacts/srcIpByte.Rdata")



head(srcIpByte)





srcIpByteQuant <- groupQuantile(srcIpByte, "type")
# quantile plot by host type
xyplot(log10(Freq) ~ p | type, data = srcIpByteQuant, layout = c(7, 1))



# look at distribution for workstations only
wFreq <- log2(subset(srcIpByteQuant, type == "Workstation")$Freq)
histogram(~ wFreq, breaks = 100, col = "darkgray", border = "white")



subset(srcIpByteQuant, Freq > 2^20 & type == "Workstation")



histogram(~ wFreq[wFreq < 20], breaks = 30, col = "darkgray", border = "white")





set.seed(1234)



library(mixtools)
mixmdl <- normalmixEM(wFreq[wFreq < 20], mu = c(16.78, 17.54, 18.2))
plot(mixmdl, which = 2, main2 = "", breaks = 50)
breakPoints <- c(17.2, 17.87)
abline(v = breakPoints)





# categorize IPs
srcIpByte$byteCat <- cut(log2(srcIpByte$Freq), 
                         breaks = c(0, breakPoints, 100), labels = c("low", "mid", "high"))

# create CIDR for subnets
srcIpByte$cidr24 <- ip2cidr(srcIpByte$firstSeenSrcIp, 24)

# tabulate by CIDR and category
cidrCatTab <- xtabs(~ cidr24 + byteCat, data = subset(srcIpByte, type == "Workstation"))
cidrCatTab



# mosaic plot
plot(cidrCatTab, color = tableau10[1:3], border = FALSE, main = NA)



srcByteQuant <- groupQuantile(
  subset(srcIpByte, type == "Workstation" & Freq < 2^20), "cidr24")

xyplot(log2(Freq) ~ p | cidr24, data = srcByteQuant,
       panel = function(x, y, ...) {
         panel.xyplot(x, y, ...)
         panel.abline(h = breakPoints, lty = 2, col = "darkgray")
       },
       between = list(x = 0.25),
       layout = c(6, 1)
)



# see if there are any inside-inside connections
srcDestInsideTab <- drXtabs(~ srcCat + destCat, data = nfRaw, 
                            transFn = function(x) {
                              x$srcCat <- "outside"
                              x$srcCat[grepl("^172", x$firstSeenSrcIp)] <- "inside"
                              x$destCat <- "outside"
                              x$destCat[grepl("^172", x$firstSeenDestIp)] <- "inside"
                              x
                            })
save(srcDestInsideTab, file = "data/artifacts/srcDestInsideTab.Rdata")



srcDestInsideTab





# get a data frame of all inside to inside connections
in2in <- recombine(nfRaw, 
                   apply = function(x) {
                     srcIn <- grepl("^172", x$firstSeenSrcIp)
                     destIn <- grepl("^172", x$firstSeenDestIp)
                     x[srcIn & destIn,]
                   }, 
                   combine = combRbind())
save(in2in, file = "data/artifacts/in2in.Rdata")



in2in[1:10, 1:5]



otherIPs <- subset(hostList, type == "Other 172.*")$IP
ind <- which(!(
  in2in$firstSeenSrcIp %in% otherIPs | 
    in2in$firstSeenDestIp %in% otherIPs))

ind



in2in[ind,1:5]



subset(hostList, IP %in% c("172.30.0.3", "172.30.1.94"))



dsq <- quantile(nfRaw, var = "durationSeconds")
save(dsq, file = "data/artifacts/dsq.Rdata")



xyplot(log2(q + 1) ~ fval * 100, data = dsq, type = "p",
       xlab = "Percentile",
       ylab = "log2(duration + 1) (seconds)",
       panel = function(x, y, ...) {
         panel.grid(h=-1, v = FALSE)
         panel.abline(v = seq(0, 100, by = 10), col = "#e6e6e6")
         panel.xyplot(x, y, ...)
         panel.abline(h = log2(1801), lty = 2)
       }
)



dsqSrcType <- quantile(nfRaw, var = "durationSeconds", by = "type",
                       preTransFn = function(x) {
                         mergeHostList(x[,c("firstSeenSrcIp", "durationSeconds")], "firstSeenSrcIp")
                       },
                       params = list(mergeHostList = mergeHostList, hostList = hostList)
)
save(dsqSrcType, file = "data/artifacts/dsqSrcType.Rdata")



xyplot(log2(q + 1) ~ fval * 100 | group, data = dsqSrcType, type = "p",
       xlab = "Percentile",
       ylab = "log2(duration + 1)",
       panel = function(x, y, ...) {
         panel.abline(v = seq(0, 100, by = 10), col = "#e6e6e6")
         panel.xyplot(x, y, ...)
         panel.abline(h = log2(1801), lty = 2)
       },
       layout = c(7, 1)
)



dsqDestType <- quantile(nfRaw, var = "durationSeconds", by = "type",
                        preTransFn = function(x) {
                          mergeHostList(x[,c("firstSeenDestIp", "durationSeconds")], "firstSeenDestIp")
                        },
                        params = list(mergeHostList = mergeHostList, hostList = hostList)
)
save(dsqDestType, file = "data/artifacts/dsqDestType.Rdata")



dsqType <- make.groups(source = dsqSrcType, dest = dsqDestType)
xyplot(log2(q + 1) ~ fval * 100 | group, groups = which, data = dsqType, type = "p",
       xlab = "Percentile",
       ylab = "log2(duration + 1)",
       panel = function(x, y, ...) {
         panel.abline(v = seq(0, 100, by = 10), col = "#e6e6e6")
         panel.xyplot(x, y, ...)
         panel.abline(h = log2(1801), lty = 2)
       },
       layout = c(8, 1),
       auto.key = TRUE
)







topPorts <- as.integer(names(commonPortList))



dsqPort <- quantile(nfRaw, var = "durationSeconds", by = "port",
                    preTransFn = function(x) {
                      srcInd <- which(x$firstSeenSrcPort %in% topPorts)
                      destInd <- which(x$firstSeenDestPort %in% topPorts)
                      data.frame(
                        durationSeconds = c(x$durationSeconds[srcInd], x$durationSeconds[destInd]),
                        port = c(x$firstSeenSrcPort[srcInd], x$firstSeenDestPort[destInd])
                      )
                    }
)
save(dsqPort, file = "data/artifacts/dsqPort.Rdata")



dsqPort$group <- factor(dsqPort$group)
nms <- sapply(commonPortList[levels(dsqPort$group)], function(x) x$name)
levels(dsqPort$group) <- paste(levels(dsqPort$group), nms)
xyplot(log2(q + 1) ~ fval * 100 | group, data = dsqPort,
       xlab = "Percentile",
       ylab = "log2(duration + 1)",
       panel = function(x, y, ...) {
         panel.xyplot(x, y, ...)
         panel.abline(h = log2(1801), lty = 2)
       },
       type = c("p", "g"),
       between = list(x = 0.25, y = 0.25),
       layout = c(5, 2)
)

#### 03_dnr.R
load("data/artifacts/bigTimeAgg.Rdata")
bigTimes <- sort(unique(bigTimeAgg$timeMinute[bigTimeAgg$Freq > 1000]))

bigIPs <- c("172.20.0.15", "172.20.0.4", "172.10.0.4", "172.30.0.4")
badIPs <- c("10.138.214.18", "10.17.15.10", "10.12.15.152", "10.170.32.110", "10.170.32.181", "10.10.11.102", "10.247.106.27", "10.247.58.182", "10.78.100.150", "10.38.217.48", "10.6.6.7", "10.12.14.15", "10.15.7.85", "10.156.165.120", "10.0.0.42", "10.200.20.2", "10.70.68.127", "10.138.235.111", "10.13.77.49", "10.250.178.101")


##### ERROR plyr won't load #####
nfByHost <- divide(nfRaw, by = "hostIP",
                   preTransFn = function(x) {
                     suppressMessages(suppressWarnings(library(cyberTools)))
                     x$timeMinute <- as.POSIXct(trunc(x$date, 0, units = "mins"))
                     x <- subset(x, !(timeMinute %in% bigTimes & 
                                        firstSeenSrcIp %in% c(bigIPs, badIPs) & 
                                        firstSeenDestIp %in% c(bigIPs, badIPs)))
                     if(nrow(x) > 0) {
                       return(getHost(x))
                     } else {
                       return(NULL)
                     }
                   },
                   output = hdfsConn("/user/perk387/vastChallenge/data/nfByHost")
)
nfByHost <- updateAttributes(nfByHost)



nfByHost



plot(log10(splitRowDistn(nfByHost)))





hostTimeAgg <- recombine(nfByHost, 
                         apply = function(x) {
                           timeHour <- as.POSIXct(trunc(x$date, 0, units = "hours"))
                           res <- data.frame(xtabs(~ timeHour))
                           res$timeHour <- as.POSIXct(res$timeHour)
                           res
                         }, 
                         combine = combDdo())
save(hostTimeAgg, file = "data/artifacts/hostTimeAgg.Rdata")



hostTimeAggDF <- recombine(hostTimeAgg, 
                           apply = identity, 
                           combine = combRbind())
save(hostTimeAggDF, file = "data/artifacts/hostTimeAggDF.Rdata")



xyplot(sqrt(Freq) ~ timeHour, data = hostTimeAggDF, alpha = 0.5)



library(trelliscope)
vdbConn("vdb")



timePanel <- function(x) {
  xyplot(sqrt(Freq) ~ timeHour, data = x, type = c("p", "g"))
}
timePanel(hostTimeAgg[[1]][[2]])



timeCog <- function(x) {
  IP <- attr(x, "split")$hostIP
  curHost <- hostList[hostList$IP == IP,]
  
  list(
    hostName = cog(curHost$hostName, desc = "host name"),
    type = cog(curHost$type, desc = "host type"),
    nobs = cog(sum(x$Freq), "log 10 total number of connections"),
    timeCover = cog(nrow(x), desc = "number of hours containing connections"),
    medHourCt = cog(median(sqrt(x$Freq)), 
                    desc = "median square root number of connections"),
    madHourCt = cog(mad(sqrt(x$Freq)), 
                    desc = "median absolute deviation square root number of connections"),
    max = cog(max(x$Freq), desc = "maximum number of connections in an hour")
  )
}

timeCog(hostTimeAgg[[1]][[2]])



makeDisplay(hostTimeAgg,
            name = "hourly_count",
            group = "inside_hosts",
            desc = "time series plot of hourly counts of connections for each inside host",
            panelFn = timePanel,
            panelDim = list(width = 800, height = 400),
            cogFn = timeCog,
            lims = list(x = "same", y = "same"))



hostTimeDirAgg <- recombine(nfByHost, 
                            apply = function(x) {
                              x$timeHour <- as.POSIXct(trunc(x$date, 0, units = "hours"))
                              res <- data.frame(xtabs(~ timeHour + srcIsHost, data = x))
                              res$timeHour <- as.POSIXct(res$timeHour)
                              res$direction <- "incoming"
                              res$direction[as.logical(as.character(res$srcIsHost))] <- "outgoing"
                              subset(res, Freq > 0)
                            }, 
                            combine = combDdo())
save(hostTimeDirAgg, file = "data/artifacts/hostTimeDirAgg.Rdata")



timePanelDir <- function(x) {
  xyplot(sqrt(Freq) ~ timeHour, groups = direction, data = x, type = c("p", "g"), auto.key = TRUE)
}

makeDisplay(hostTimeDirAgg,
            name = "hourly_count_src_dest",
            group = "inside_hosts",
            desc = "time series plot of hourly counts of connections for each inside host by source / destination",
            panelFn = timePanelDir,
            panelDim = list(width = 800, height = 400),
            cogFn = timeCog,
            lims = list(x = "same", y = "same"))





bigHosts <- nfByHost[paste("hostIP=", 
                           c("172.10.2.106", "172.30.1.218", "172.20.1.23", 
                             "172.10.2.135", "172.20.1.81", "172.20.1.47", 
                             "172.30.1.223", "172.10.2.66"), sep = "")]



hostOne <- bigHosts[[1]][[2]]
hostOne <- subset(hostOne, srcIsHost)
table(hostOne$firstSeenDestPort)



hostOne$timeHour <- as.POSIXct(trunc(hostOne$date, 0, units = "hours"), tz = "UTC")
hostOneTab <- data.frame(xtabs(~ firstSeenDestPort + timeHour, data = hostOne))
hostOneTab$timeHour <- as.POSIXct(hostOneTab$timeHour)
hostOneTab <- subset(hostOneTab, Freq > 0)

xyplot(sqrt(Freq) ~ timeHour, groups = firstSeenDestPort, 
       data = hostOneTab, auto.key = TRUE, type = c("p", "g"))



subset(hostOneTab, Freq > 1500)



spike1 <- subset(hostOne, 
                 date >= as.POSIXct("2013-04-13 07:00:00", tz = "UTC") &
                   date <= as.POSIXct("2013-04-13 09:00:00", tz = "UTC") & 
                   firstSeenDestPort == 80)
table(spike1$firstSeenDestIp)





spike1$logPB <- log10(spike1$firstSeenDestPayloadBytes + 1)
spike1pbQuant <- groupQuantile(spike1, "firstSeenDestIp", "logPB")

xyplot(logPB ~ p * 100 | firstSeenDestIp, data = spike1pbQuant,
       xlab = "Percentile",
       ylab = "log10(firstSeenDestPayload + 1)",
       layout = c(11, 1),
       between = list(x = 0.25)
)



spike2 <- subset(hostOne, 
                 date >= as.POSIXct("2013-04-14 07:00:00") & 
                   date <= as.POSIXct("2013-04-14 09:00:00"))
table(spike2$firstSeenDestIp)



nrow(subset(hostOne, firstSeenDestIp == "10.1.0.100"))



{
 
  x <- nfByHost[[1]][[2]]
  
  portProp <- drLapply(nfByHost, function(x) {
    commonPortDF2 <- droplevels(subset(commonPortDF, portName %in% c("RDP", "SMTP", "SSH", "HTTP")))
    tmp <- x[,c("firstSeenDestPort", "srcIsHost")]
    names(tmp)[1] <- "port"
    tmp$dir <- "in"
    tmp$dir[tmp$srcIsHost] <- "out"
    tmp$dir <- factor(tmp$dir, levels = c("in", "out"))
    tmp <- merge(tmp, commonPortDF2)
    a1 <- data.frame(xtabs(~ portName + dir, data = tmp))
    
    tmp <- x[,c("firstSeenSrcPort", "srcIsHost")]
    names(tmp)[1] <- "port"
    tmp$dir <- "out"
    tmp$dir[tmp$srcIsHost] <- "in"
    tmp$dir <- factor(tmp$dir, levels = c("in", "out"))
    tmp <- merge(tmp, commonPortDF2)
    a2 <- data.frame(xtabs(~ portName + dir, data = tmp))
    
    res <- data.frame(t(a1$Freq + a2$Freq))
    res[1:4] <- res[1:4] / max(sum(res[1:4]), 1)
    res[5:8] <- res[5:8] / max(sum(res[5:8]), 1)
    names(res) <- paste(a1$portName, a1$dir, sep = "_")
    res
  }, combine = combRbind(), params=list(commonPortDF=commonPortDF))
  
  
  library(flexclust)
  
  #Perform k-means clustering
  
  portClust <- stepFlexclust(portProp[,-1], k = 4:12)
  plot(portClust)
  
  portClust <- kcca(portProp[,-1], k = 10)
  
  portProp2 <- merge(portProp, hostList, by.x = "hostIP", by.y = "IP")
  portProp2$cluster <- portClust@cluster
  
  subset(portProp2, cluster == 1)
}



nfPanel <- function(x) {
  x$group <- ifelse(x$firstSeenSrcIp == attributes(x)$split$hostIP, "sending", "receiving")
  x$group <- factor(x$group, levels = c("sending", "receiving"))
  x$zeroDur <- ifelse(x$durationSeconds == 0, "0 seconds", ">0 seconds")
  x$zeroDur <- factor(x$zeroDur, c("0 seconds", ">0 seconds"))
  xyplot(log10(firstSeenSrcPayloadBytes + 1) ~ log10(firstSeenDestPayloadBytes + 1) | zeroDur, groups = group, data = x, 
         auto.key = TRUE, 
         # panel = log10p1panel,
         # scales = log10p1scales,
         between = list(x = 0.25),
         grid = TRUE, logx = TRUE, logy = TRUE,
         xlab = "log10(Destination Payload Bytes + 1)",
         ylab = "log10(Source Payload Bytes + 1)"
  )
}

nfPanel(nfByHost[[1]][[2]])

nfCog <- function(x) {
  IP <- attr(x, "split")$hostIP
  curHost <- hostList[hostList$IP == IP,]
  
  c(list(
    hostName = cog(curHost$hostName, desc = "host name"),
    IP = cog(IP, desc = "host IP address"),
    type = cog(curHost$type, desc = "host type"),
    nobs = cog(log10(nrow(x)), "log 10 total number of connections"),
    propZeroDur = cog(length(which(x$durationSeconds == 0)), desc = "proportion of zero duration connections")
  ),
    cogScagnostics(log10(x$firstSeenSrcPayloadBytes + 1), 
                   log10(x$firstSeenDestPayloadBytes + 1)))
}

nfCog(nfByHost[[1]][[2]])



makeDisplay(nfByHost,
            name = "srcPayload_vs_destPayload",
            panelFn = nfPanel,
            cogFn = nfCog,
            panelDim = list(width = 900, height = 600))



nfByExtHost <- divide(nfByHost, by = "extIP",
                      preTransFn = function(x) {
                        x$extIP <- x$firstSeenSrcIp
                        x$extIP[x$srcIsHost] <- x$firstSeenDestIp[x$srcIsHost]
                        x
                      },
                      output = hdfsConn("data/nfByExtHost"),
)
nfByExtHost <- updateAttributes(nfByExtHost, control = clc)





nfByTime <- divide(nfByHost, by = "time10",
                   preTransFn = function(x) {
                     tmp <- paste(substr(x$date, 1, 15), "0:00", sep = "")
                     x$time10 <- as.POSIXct(tmp, tz = "UTC")
                     x
                   },
                   output = hdfsConn("data/nfByTime"),
)
nfByTime <- updateAttributes(nfByTime)







