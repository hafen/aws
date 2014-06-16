########################################################
### Activity 2: Raw Data ETL and Data Familiarization
########################################################

########################################################
### Activity 2.1
### Session initialization
########################################################

# load required packages and initialize Rhipe
#!! added these two 
library(devtools)
# install_github("vastChallenge", "hafen", subdir = "package")
install_github("example-vast-challenge", "tesseradata", subdir = "package", ref = "gh-pages")
library(datadr)
library(Rhipe)
library(cyberTools)
rhinit()
# set Rhipe runner (specific to this cluster)
#!! did not use
#rhoptions(runner = "/share/apps/R/3.0.2/bin/R CMD /share/apps/R/3.0.2/lib64/R/library/Rhipe/bin/RhipeMapReduce --slave --silent --vanilla")

# set time zone to "UTC" for use with dates in the data
Sys.setenv(TZ = "UTC")

# set working directories on local machine and HDFS
#!! changed dir
setwd("/home/vagrant")
# change "hafe647" to your username
#!! changed dir
hdfs.setwd("/user/hadoop/vast")

# make sure data sample is present locally
list.files(pattern=".csv")

# make sure raw text data has been copied to HDFS
rhls("raw/nf")


########################################################
### Activity 2.2
### Read NetFlow csv data to R data frames
########################################################

# read in 10 rows of NetFlow data from local disk
nfHead <- read.csv("nf-week2-sample.csv", nrows = 10, stringsAsFactors = FALSE)

# look at first 10 rows for some variables
nfHead[1:10,3:7]

# look at structure of the data
str(nfHead)

# make new date variable
#!! error with date:
#!! Error in as.POSIXlt.character(x, tz, ...) : character string is not in a standard unambiguous format
#!! added as.double
nfHead$date <- as.POSIXct(as.double(nfHead$TimeSeconds), origin = "1970-01-01", tz = "UTC")

# remove old time variables
nfHead <- nfHead[,setdiff(names(nfHead), c("TimeSeconds", "parsedDate"))]

# make a nice transformation function based on our previous steps
#!! added as.character
nfTransform <- function(x) {
  x$date <- as.POSIXct(as.double(x$TimeSeconds), origin = "1970-01-01", tz = "UTC")
  x[,setdiff(names(x), c("TimeSeconds", "parsedDate"))]
}

# initiate a connection to existing csv text file on HDFS
#!! permission issues
csvConn <- hdfsConn("raw/nf", type = "text")

# initiate a new connection where parsed NetFlow data will be stored
#!! permission issues
nfConn <- hdfsConn("nfRaw")

# look at the connection
nfConn

# read in NetFlow data
#!! gives error:
#!! Error in as.POSIXlt.character(x, tz, ...) : character string is not in a standard unambiguous format 
nfRaw <- drRead.csv(csvConn, output = nfConn, postTransFn = nfTransform)


########################################################
### Activity 2.3
### Getting Familiar with Distributed Data Objects
########################################################

# look at the nfRaw object
nfRaw

# reload "nfRaw" by loading the connection as a ddf
nfRaw <- ddf(hdfsConn("nfRaw"))

# get missing attributes
nfRaw <- updateAttributes(nfRaw)

# look at the updated nfRaw object
nfRaw

# see what variables are available
names(nfRaw)

# get total number of rows
nrow(nfRaw)

# look at the structure of the first key-value pair
str(nfRaw[[1]])

# look at summaries (computed from updateAttributes)
summary(nfRaw)
