setwd("Y:/SAB-RESEARCH-UNIT/02Work/ResearchProjects/Validation/Sample Data")

library("data.table")
library("lubridate")
library("RcppRoll")
library("dplyr")
library("zoo")

# read in the data 
df1 <- read.csv("03162021Assessment_clean.csv", stringsAsFactors = F) 
df2 <- read.csv("03172021Case_clean.csv", stringsAsFactors = F) 

# order the case file data by xnmbr 
df2 <- df2[order(df2$xnmbr),] 

# select xnmbr, court case number, grant date, and closing date 

df3 <- df2[,c(6, 13, 15, 16)]
df3 <- df3[order(df3$xnmbr),]

# extract month and year from case grant date and sup closing date from case files 

df3$Month_grant <- month(as.POSIXlt(df3$case_grant_date, format="%m/%d/%Y"))
df3$Year_grant <-  year(as.POSIXlt(df3$case_grant_date, format="%m/%d/%Y"))
df3$Month_closing <- month(as.POSIXlt(df3$sup_closing_date, format="%m/%d/%Y"))
df3$Year_closing <-  year(as.POSIXlt(df3$sup_closing_date, format="%m/%d/%Y"))

# Inspecting the grand date and closing date 

table(df3$Year_grant)
table(df3$Year_closing)

# there are five cases where closing date happened before grant date

which(grepl(2009, df3$Year_closing)) # 35595
which(grepl(2010, df3$Year_closing)) # 18759 44830 46973 84594

## those cases are removed from the analytical data 

df4 <- df3[df3$Year_closing >= 2011,]

# remove NA cases from the analytical data 
# table(is.na(df4[1]))
# FALSE   TRUE 
# 191232  41213

df4 <- na.omit(df4, cols="xnmbr")

# Inspect month-year combination 

table(df4$Year_grant, df4$Month_grant)
table(df4$Year_closing, df4$Month_closing)

# inspect missing from the Year_grant and MOnth_grant 

table(is.na(df4[5]))
table(is.na(df4[6]))

####################################################
####### Expand grid 
####################################################

#################Rewrite Recode#################################
df4$year_month <- paste(df4$Year_grant, df4$Month_grant, sep = "_")

df4$countingmo <- df4$year_month

# create a lookup vector
x <- rep(2011:2020, each = 12)
y <- rep(2021, each = 2)
xplusy <- c(x,y)
z <- rep(1:12,11,length.out=122)
xplusy_z <- paste(xplusy, z, sep="_")

# create a list
lookup_list <- 1:122
names(lookup_list) <- xplusy_z

#recode countingmo 
df4$countingmo <- do.call(dplyr::recode, c(list(df4$countingmo), lookup_list))

#################Rewrite Recode#################################

all_combinations <- expand.grid(xplusy_z, xnmbr=unique(df4$xnmbr))

# Sort both datasets to prepare for joining
df4 <- df4[order(df4$xnmbr, df4$countingmo),]
all_combinations <- all_combinations[order(all_combinations$xnmbr, all_combinations$countingmo),]

# convert data types 
all_combinations$countingmo <- as.integer(all_combinations$countingmo)
all_combinations$xnmbr <- as.character(all_combinations$xnmbr)

# join datasets
df5 <- df4[all_combinations, on=.(countingmo, xnmbr)]

#remove assessment done outside 01/02/2011 - 02/02/2021 range
assmt_nona <- assmt[!is.na(assmt$countingmo),]

# join assessment with expand grid
setDT(df5)
setDT(assmt_nona)
assmt_panel <- assmt_nona[df5, on=.(xnmbr,countingmo)]


###########################################
## remove duplicate rows on the same day
# 5680 cases where an individual have multiple 
# cases on the same case_grant_date
###########################################

# there were 109924 cases (57%) without recidivism
norecidivism <- setDT(cases)[, nrows := .N,by=xnmbr][nrows <= 1,]

# keep one case on the same day per individual by selecting the first case
temp1<- cases %>% 
  group_by(xnmbr, case_grant_date) %>% 
  mutate(dupe = n()>1) %>%
  filter (dupe == TRUE) %>% filter(row_number() == 1)

temp2 <- cases %>% 
  group_by(xnmbr, case_grant_date) %>% 
  mutate(dupe = n()>1) %>%
  filter (dupe == FALSE) 

#the merged case_uniq file only allows for one case per individual per case_grant_date
cases_uniq <- rbind(temp1, temp2)

# there were 1029 cases where case_grant_date and sup_closing_date is on the same day - flash incarcerations?
nrow(cases_uniq[case_grant_date == sup_closing_date,])


###########################################
## add countingmo to assessment data
###########################################
# extra year and month from assessment date
assmt$month_assmnt <- month(as.POSIXlt(assmt$assmnt_date, format="%m/%d/%Y"))
assmt$year_assmnt <-  year(as.POSIXlt(assmt$assmnt_date, format="%m/%d/%Y"))

#concatenate year and month
assmt$year_month <- paste(assmt$year_assmnt, assmt$month_assmnt, sep="_")

#recode year_month according to lookup list
assmt$countingmo <- assmt$year_month
assmt$countingmo <- do.call(dplyr::recode, c(list(assmt$countingmo), lookup_list))


###########################################
## add countingmo to case file
###########################################
# extra year and month from assessment date
cases_uniq$month_grant <- month(as.POSIXlt(cases_uniq$case_grant_date, format="%m/%d/%Y"))
cases_uniq$year_grant <-  year(as.POSIXlt(cases_uniq$case_grant_date, format="%m/%d/%Y"))

#concatenate year and month
cases_uniq$year_month <- paste(cases_uniq$year_grant, cases_uniq$month_grant, sep="_")

#recode year_month according to lookup list
cases_uniq$countingmo <- cases_uniq$year_month
cases_uniq$countingmo <- do.call(dplyr::recode, c(list(cases_uniq$countingmo), lookup_list))


###########################################
## merge cases_uniq with assmt_panel
###########################################

finalDT <- cases_uniq[assmt_panel, on =.(xnmbr, countingmo)]
finalDT <- unique(finalDT)

###### Experiment on temporary data ######################
# examine the data
finalDT_temp <- head(finalDT, 1220)

# fill the NAs in some cols
finalDT_temp <- finalDT_temp %>% group_by(xnmbr) %>% fill(race, drug, sex_registration, arson, .direction = "downup")

# calculate difference in months between case grant and case closing dates
setDT(finalDT_temp)[, interval := (interval(case_grant_date, sup_closing_date) %/% months(1))]

# fill downward to a certain number of rows : case_grant_date and sup_closing_date 














