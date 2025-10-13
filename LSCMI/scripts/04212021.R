setwd("Y:/SAB-RESEARCH-UNIT/02Work/ResearchProjects/Validation/Sample Data")
install.packages(c("data.table", "lubridate", "zoo", "dplyr","readr", "Hmisc","readr","fuzzyjoin"))
lapply(c("data.table", "lubridate", "zoo", "dplyr","readr", "Hmisc", "readr", "lme4"), require, character.only=TRUE)

# read the fwf data file
cases1 <- read_fwf("JusticeMetrics/Probation_Case_20210311.txt", fwf_cols("submit_date"=c(1,10), "agency"=c(11,19),"source"=c(20,22),"xnmbr"=c(23,31),"prob_stts_code"=c(32,33),"prim_chrg_code"=c(34,35),"pri_chrg_stt"=c(36,53),"pri_chrg_id"=c(54,56),"pri_chrg_deg"=c(57,57),"pri_chrg_lvl"=c(58,58),"crt_case_nmbr"=c(59,71),"case_stts_code"=c(72,73),"case_grant_date"=c(74,83),"sup_close_date"=c(84,93),"race"=c(94,94),"area_office_code"=c(95,100),"area_office_name"=c(101,120),"area_office_addr"=c(121,155),"area_office_city"=c(156,175),"area_office_state"=c(176,177),"area_office_zip"=c(178,186),"cnt_grant_type_ind"=c(187,207),"supvn_caseld_type"=c(208,210),"homeless_ind"=c(211,211),"dpo_lstname"=c(212,231),"dpo_1stname"=c(232,246),"dpo_nmbr"=c(247,256),"dpo_email"=c(257,306),"drug"=c(307,307),"sex"=c(308,308),"arson"=c(309,309),"cjis_charge_code"=c(310,311),"record_type"=c(312,312)), col_types = cols("pri_chrg_deg"=col_integer()))

disp1 <- read_fwf("JusticeMetrics/Probation_Disposition_20210311.txt", fwf_cols("submit_date"=c(1, 10), "agency"=c(11,19),"source"=c(20,22),"xnmbr"=c(23,31),"crt_case_nmbr"=c(32,44),"cnt_nmbr"=c(45,47),"dispo_date"=c(48,57),"dispo_code"=c(58,59),"dispo_desc"=c(60,89),"record_type"=c(90,90)))

assmnt <- read_fwf("JusticeMetrics/Probation_Assessment_20210311.txt", fwf_cols("submit_date"=c(1,10), "xnmbr"=c(23,31), "assmnt_date"=c(57,66),"total_score"=c(67,68),"criminal_hist"=c(69,70), "edu_emp"=c(71,72),"fam_marital"=c(73,74),"leisure_rec"=c(75,76),"company"=c(77,78),"alcohol_drug"=c(79,80),"pao"=c(81,82), "antisocial"=c(83,84)), col_types=cols("assmnt_date"=col_date(format="%m/%d/%Y"))) 

# 170068 unique individuals 
disp1[, .N, by=xnmbr]
#170239 unique individuals
cases1[, .N, by=xnmbr]

## identify cases that had one disposition only
# disp1[,.N, by=xnmbr][N>1] -> one_disp
## remove one_disp from disp1
# disp <- disp1[!xnmbr %in% one_disp$xnmbr,]

# merge disposition data with cases data
#inner join
case_disp0 <- merge(disp1, cases1)
#left/right join
case_disp1 <- merge(cases1, disp1, by.x=c("xnmbr", "crt_case_nmbr"), by.y=c("xnmbr", "crt_case_nmbr"), all.x=TRUE,all.y=FALSE)
case_disp11 <- merge(disp1, cases1, by.x=c("xnmbr", "crt_case_nmbr"), by.y=c("xnmbr", "crt_case_nmbr"), all.x=TRUE,all.y=FALSE)
# full join
case_disp2 <- merge(disp1, cases1,by.x=c("xnmbr", "crt_case_nmbr"), by.y=c("xnmbr", "crt_case_nmbr"), all.x=TRUE,all.y=TRUE, allow.cartesian = TRUE)

# convert to date type
case_disp0$case_grant_date <- as.Date(as.POSIXlt(case_disp0$case_grant_date, format="%m/%d/%Y"))
case_disp0$sup_close_date <- as.Date(as.POSIXlt(case_disp0$sup_close_date, format="%m/%d/%Y"))
case_disp0$dispo_date <- as.Date(as.POSIXlt(case_disp0$dispo_date, format="%m/%d/%Y"))

######################
### PRC Cohorts
#####################

# calculate the number probationers within a PRC cohort (removing cases that had one disposition)
case_disp0[year_grant == 2017 & cnt_grant_type_ind == "PRC", .N, by=xnmbr]
case_disp0[year_grant == 2011 & cnt_grant_type_ind == "MANDATORY SUPERVISION", .N, by=xnmbr]
case_disp0[year_grant == 2017 & cnt_grant_type_ind == "FORMAL PROBATION", .N, by=xnmbr]

# calculate the number of flash incarcerations within a PRC cohort
case_disp0[year_grant == 2017 & cnt_grant_type_ind == "PRC" & dispo_desc %in% c("FLASH INCARCERATION", "ARRST WRRNT RECAL-FLASH INCARC") & dispo_date <= (case_grant_date %m+% months(36)),  .N, by=.(xnmbr)]

######################
### MAND SUPRVN Cohorts
#####################

# put revocation (with remand to custody) codes into a vector
revoke_code <- c("DIV/DEJ REVOKED", "DIV/DEJ REVOKED - BWI","P47 PROB REV-PROP36TERM-BWI","P47 PROB REV-PROP36TERM-CONT","P47 PROBREV-P36TERM-PROBDENCJ","PROB REV-O/C COMMITTED CRC","PROB REV-O/C DEPT MNTAL HEALTH","PROB REV-O/C REFERRED DEPT 95","PROB REVOKED - O/C 1203.03 PC","PROB REVOKED - O/C 707.02 WIC","PROB REVOKED BWI-PROP36 TERM","PROB REVOKED-PRISON 1731.5 WIC","PROB REVOKED-PRISON STAYED","PROBATION REVOKED - BWI","PROBATION REVOKED - IN CUSTODY","PROBATION REVOKED - JAIL","PROBATION REVOKED - PRISON","REV-CJ 1170H(1) AND 1170H(2)")

# calculate the number of revocations in a mandatory supervision cohort
case_disp0[year_grant == 2015 & cnt_grant_type_ind == "FORMAL PROBATION" & dispo_desc %in% revoke_code & dispo_date <= (case_grant_date %m+% months(36)),  .N, by=.(xnmbr)] 

case_disp0[year_grant == 2011 & cnt_grant_type_ind == "FORMAL PROBATION" & dispo_desc == "FLASH INCARCERATION" & dispo_date <= (case_grant_date %m+% months(36)),  .N, by=.(xnmbr)]

######################
### Dummy Coding
#####################

# recode flash incarcerations
case_disp0$flash_inc <- 0
case_disp0$flash_inc[case_disp0$dispo_desc == "FLASH INCARCERATION"] <- 1

#recode revoke
case_disp0$revoked <- 0
case_disp0$revoked[case_disp0$dispo_desc %in% revoke_code ] <- 1

######################
### Merge with lscmi
######################

case_disp_assmnt <- assmnt[case_disp0, 
                           .(i.xnmbr, i.crt_case_nmbr,i.cnt_nmbr, i.dispo_date, i.dispo_code, i.dispo_desc, i.prob_stts_code, i.prim_chrg_code, i.pri_chrg_stt, i.pri_chrg_id, i.pri_chrg_deg, i.pri_chrg_lvl, i.case_stts_code, i.case_grant_date, i.sup_close_date, i.race, i.cnt_grant_type_ind, i.flash_inc, i.revoked, i.drug, i.sex, i.arson, i.cjis_charge_code, i.month_grant, i.year_grant, i.month_closing, i.year_closing, x.assmnt_date, x.total_score, x.criminal_hist, x.edu_emp, x.fam_marital, x.leisure_rec, x.company, x.alcohol_drug, x.pao, x.antisocial), 
                        nomatch=0,on = .(xnmbr = xnmbr, assmnt_date <= sup_close_date, assmnt_date >= case_grant_date)]

# remove redundant rows
case_disp_assmnt <- unique(case_disp_assmnt)

# move assmnt_date & total score forward
case_disp_assmnt <- case_disp_assmnt %>% relocate(c(x.assmnt_date, x.total_score), .after=i.dispo_date)
case_disp_assmnt <- case_disp_assmnt %>% relocate(c(i.case_grant_date, i.sup_close_date), .after=i.cnt_nmbr)

# reorder columns 
case_disp_assmnt <-setDT(case_disp_assmnt)[order(i.xnmbr,i.crt_case_nmbr,i.case_grant_date,i.dispo_date,x.assmnt_date)]


###########################################################
## data analysis
##########################################################

# subset data for PRC cohorts  2011 to 2017
prc_cohort_2011 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2011 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]
prc_cohort_2012 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2012 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]
prc_cohort_2013 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2013 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]
prc_cohort_2014 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2014 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]
prc_cohort_2015 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2015 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]
prc_cohort_2016 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2016 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]
prc_cohort_2017 <- case_disp_assmnt[case_disp_assmnt$i.year_grant == 2017 & case_disp_assmnt$i.cnt_grant_type_ind== "PRC",]

# unconditional means models
model0_fit <- glmer(formula = i.flash_inc ~ 1 + (1|i.xnmbr), data=prc_cohort_2011, family=binomial, nAGQ = 0, na.action=na.exclude)
summary(model0_fit)
model0.0_fit <- glmer(formula = i.revoked ~ 1 + (1|i.xnmbr), data=prc_cohort_2015, family=binomial, nAGQ = 0, na.action=na.exclude)
summary(model0.0_fit)

# intraclass correlation for prc 2011 cohort is 9.7%
# residual variance is fixed to pi ^2 /3
icc_prc_2011 <- 0.3523/(0.3523 + 3.14159^2/3)
icc_prc_2012 <- 0.2491/(0.2491 + 3.14159^2/3)
icc_prc_2013 <- 0.2497/(0.2497 + 3.14159^2/3)
icc_prc_2014 <- 0.3355/(0.3355 + 3.14159^2/3)
icc_prc_2015 <- 0.7239/(0.7239 + 3.14159^2/3)
icc_prc_2016 <- 3.494/(3.494 + 3.14159^2/3)
icc_prc_2017 <- 11.92/(11.92 + 3.14159^2/3)

# test against the single level logistic model 
# there is evidence of between individual variance is non-zero
model0_null <- glm(formula = flash_inc ~ 1, data = prc_cohort_2017, family=binomial, na.action=na.exclude)
logLik(model0_null) - logLik(model0_fit)  # -320.47 is significant at 1 df chi-sq distribution

# recode assessment occasion according to a fixed schedule
prc_cohort_2011$assmnt_year_month <- paste(year(prc_cohort_2011$x.assmnt_date), month(prc_cohort_2011$x.assmnt_date), sep = "_")
prc_cohort_2011$assmnt_seq <- prc_cohort_2011$assmnt_year_month
prc_cohort_2011$assmnt_seq<- do.call(dplyr::recode, c(list(prc_cohort_2011$assmnt_year_month), lookup_list))

# unconditional growth model (random intercept model)
model1_fit <- glmer(formula = flash_inc ~ 1 + assmnt_seq + (1|i.xnmbr), data=prc_cohort_2017, family=binomial, nAGQ = 0, na.action=na.exclude)
summary(model1_fit)
logLik(model0_fit) - logLik(model1_fit) # df=2

# use x.total_score and assmnt_seq to predict flash incarceration
# beta = 0.018, p <.001
model3_fit <- glmer(formula = flash_inc ~ 1 + x.total_score + (1|i.xnmbr), data=prc_cohort_2011, family=binomial, nAGQ = 0, na.action=na.exclude)
summary(model3_fit)
logLik(model0_fit) - logLik(model3_fit) # df=2

# introduce other covariates and factors
model4_fit <- glmer(formula = flash_inc ~ 1 + x.total_score + (1|i.xnmbr), data=prc_cohort_2011, family=binomial, nAGQ = 0, na.action=na.exclude)
summary(model4_fit)


