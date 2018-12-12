library(ggplot2)
draw.time <- function(stepData, extraTitle) {
    stepData$grp <- paste(stepData$NumWorker, "workers", stepData$NumCPU, "CPUS")
    g <- ggplot(stepData, aes(fill=stepData$grp, x=factor(stepData$input), y=stepData$time))
    g <- g + geom_bar(position="dodge", stat="identity")
    tmp  <- paste("Time Scaling", extraTitle)
    g <- g + labs(title=tmp, x="Input", y="Time (sec)", fill="Cluster Type")
    g
}

fullData <- read.csv("./results.csv", header=TRUE, sep=",")

attach(fullData)

pdf("./OneGraph.pdf")
draw.time(fullData, "for Clusters")
