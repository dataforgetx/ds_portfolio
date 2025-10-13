install.packages("tidyverse")
library(tidyverse)

########################################
### Global params and data wrangling
########################################

# only change the start and end dates within quotation marks 
date_range <- c("2018-01-01", "2018-01-30")

# read in sample data (for demonstration purposes)
# real staff data comes from oracle, sql server databases, and flat files
df <- read.csv('data/CombinedCSV.csv', header=T, stringsAsFactors = F, na.strings = c("", NA))

# wrapper function
wrapper <- function(dat) {function(func) {map_dbl(dat, func)}}

#make a copy of original data frame
df_c <- df

# convert string to date format
df_c$INCIDENT_DATE <- as.POSIXct(df_c$INCIDENT_DATE, format="%m/%d/%Y %H:%M:%S")

#rename and remove useless cols
names(df_c)[c(11, 12)] <- c("staff", "youth")
df_c[,8] <- NULL

# regular expression
str_pat <- '\\/P'

# RECODE INCIDENT 
df_c$INT_LEVEL2 <- recode(df_c$INT_LEVEL, "C6-SRLevel C6-Sr" ="0",
                          "A1Level A1" = "1", "A2Level A2" = "2",  "B3Level B3" = "3", 
                          "B4Level B4" = "4", "C5Level C5" = "5", "C6-OCLevel C6-Oc" ="6")

df_c$INT_LEVEL2 <- as.numeric(df_c$INT_LEVEL2)

########################################
### Metric 1
########################################

# subset one month data to verify algorithm
metric1 <- df_c %>% 
  filter(INCIDENT_DATE >= date_range[1] & INCIDENT_DATE <= date_range[2]) %>% 
  arrange(youth, staff, INCIDENT_DATE) %>% 
  select(youth, staff, everything())
  
# remove rows missing intervention 
metric1 <- metric1[!is.na(metric1$INT_LEVEL),] 

# calculate the max per youth, staff, incident
metric1 <- metric1 %>% 
  group_by(youth, SCM_ID, staff) %>% 
  mutate(MAX_INTLVL = max(INT_LEVEL2, na.rm = TRUE)) %>% 
  filter(row_number() == 1) %>% 
  # calculate the mean intervention for each staff per youth
  ungroup() %>% 
  group_by(youth,staff) %>% 
  mutate(STAFF_AVG = mean(MAX_INTLVL, na.rm=TRUE))  %>% 
  filter(row_number() == 1)

# difference between one's average intervention and the mean of other staff with a youth
metric1 <- metric1 %>% 
  group_by(youth) %>% 
  # filter out youth that had only one-staff intervention
  mutate(number= n()) %>% 
  filter(number >= 2) %>% 
  select(-number) %>% 
  mutate(diff = wrapper(row_number())(~{STAFF_AVG[.] - mean(MAX_INTLVL[staff != staff[.]])}))

# export metric1 as csv
write.csv(metric1, 'metric1.csv')

# graph the top 10 staff with highest use of force intervention
temp <- metric1 %>% 
  group_by(staff, diff) %>% 
  mutate(freq = n()) %>% 
  arrange(desc(diff, freq)) %>% 
  head(10)  
  
temp %>% 
  group_by(staff) %>% 
  ggplot(aes(x=diff, y=freq, fill=staff)) +
  geom_col() +
  xlab("Difference in intervention from group average")+
  ylab('How many times intervention was used') +
  facet_wrap(~ staff, nrow=2) +
  ylim(0,2) + 
  coord_flip()

########################################
### Metric 2
########################################

# subset one month data to verify algorithm
metric2 <- df_c %>% 
  filter(INCIDENT_DATE >= date_range[1] & INCIDENT_DATE <= date_range[2]) %>% 
  arrange(SCM_ID, staff) %>% 
  select(SCM_ID, staff, INT_LEVEL2, everything())

# rows missing intervention removed
metric2$INT_LEVEL2 <- as.numeric(metric2$INT_LEVEL2)
metric2 <- metric2[!is.na(metric2$INT_LEVEL),] 

metric2 <- metric2 %>% 
  group_by(SCM_ID) %>% 
  # remove incidents that had only one staff
  filter(n_distinct(staff) > 1) %>% 
  # calculate the highest intervention per incident per staff
  ungroup() %>% 
  group_by(SCM_ID,staff) %>% 
  mutate(MAX_INTLVL = max(INT_LEVEL2, na.rm=T)) %>% 
  # select one row per incident per staff
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  group_by(SCM_ID) %>% 
  mutate(n=row_number()) %>% 
  # calculate the diff between a staff and the average of all other staff involved in an incident
  mutate(diff = wrapper(n)(~{MAX_INTLVL[.] - mean(MAX_INTLVL[staff != staff[.]])})) %>% 
  select(-n)

# export metric2 as csv
write.csv(metric2, 'metric2.csv')

########################################
### Metric 3
########################################

metric3 <- df_c %>% 
  filter(INCIDENT_DATE >= date_range[1] & INCIDENT_DATE <= date_range[2]) %>% 
  # filter out SIR cases
  filter(str_detect(SCM_INCIDENT_NUMBER, str_pat)) %>% 
  select(staff, INCIDENT_DATE, ACTUAL_INVOLVED_FLAG, WITNESS_FLAG, NOT_INVOLVED_FLAG, everything()) %>%
  group_by(staff, SCM_ID) %>% 
  summarise(actual_involved = sum(ACTUAL_INVOLVED_FLAG), 
            witness = sum(WITNESS_FLAG),
            not_involved = sum(NOT_INVOLVED_FLAG)) %>% 
  mutate(actual_witness = round(actual_involved / witness, 2),
         actual_not = round(actual_involved / not_involved,2))

# export metric3 as csv 
write.csv(metric3, 'metric3.csv') 
  
