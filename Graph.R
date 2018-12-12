library(ggplot2)
draw.timeP <- function(stepData, extraTitle) {
    stepData$grp <- paste(stepData$NumWorker, "workers", stepData$NumCPU, "CPUS")
    g <- ggplot(stepData, aes(fill=stepData$grp, x=factor(stepData$input), y=stepData$time))
    g <- g + geom_bar(position="dodge", stat="identity")
    tmp  <- paste("Time Scaling", extraTitle)
    g <- g + labs(title=tmp, x="Input", y="Time (sec)", fill="Cluster Type")
    g
}

draw.time  <- function(stepData, extraTitle) {
    g<- ggplot(stepData, aes(x=factor(Filename), y=time))
    g <- g + geom_col(fill="plum3")
    tmp  <- paste("Time Scaling", extraTitle)
    g <- g + labs( title=tmp, x="Input", y="Time (sec)")
    g
}

fullData <- read.csv("./results.csv", header=TRUE, sep=",")

attach(fullData)

pdf("./OneGraph.pdf")
draw.timeP(fullData, "for Clusters")


pdf("./CrapGraph.pdf")
crapData <- read.csv("sequential_results.csv", header=TRUE, sep=",")
draw.time(crapData, "for Sequential")
