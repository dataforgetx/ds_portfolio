
# load the packages
lapply(c("data.table", "tidyverse","lme4"), require, character.only=TRUE)

############################################
### Read and Clean Data
############################################
cases1 <- read_fwf("Raw_Data/Probation_Case_20210311.txt", 
                   fwf_cols("submit_date"=c(1,10), 
                            "agency"=c(11,19),
                            "source"=c(20,22),
                            "xnmbr"=c(23,31),
                            "prob_stts_code"=c(32,33),
                            "prim_chrg_code"=c(34,35),
                            "pri_chrg_stt"=c(36,53),
                            "pri_chrg_id"=c(54,56),
                            "pri_chrg_deg"=c(57,57),
                            "pri_chrg_lvl"=c(58,58),
                            "crt_case_nmbr"=c(59,71),
                            "case_stts_code"=c(72,73),
                            "case_grant_date"=c(74,83),
                            "sup_close_date"=c(84,93),
                            "race"=c(94,94),
                            "area_office_code"=c(95,100),
                            "area_office_name"=c(101,120),
                            "area_office_addr"=c(121,155),
                            "area_office_city"=c(156,175),
                            "area_office_state"=c(176,177),
                            "area_office_zip"=c(178,186),
                            "cnt_grant_type_ind"=c(187,207),
                            "supvn_caseld_type"=c(208,210),
                            "homeless_ind"=c(211,211),
                            "dpo_lstname"=c(212,231),
                            "dpo_1stname"=c(232,246),
                            "dpo_nmbr"=c(247,256),
                            "dpo_email"=c(257,306),
                            "drug"=c(307,307),
                            "sex"=c(308,308),
                            "arson"=c(309,309),
                            "cjis_charge_code"=c(310,311),
                            "record_type"=c(312,312)), 
                   col_types = cols("pri_chrg_deg"=col_character()), na = c("", "NA"))

disp1 <- read_fwf("Raw_Data/Probation_Disposition_20210311.txt", 
                  fwf_cols("submit_date"=c(1, 10), 
                           "agency"=c(11,19),
                           "source"=c(20,22),
                           "xnmbr"=c(23,31),
                           "crt_case_nmbr"=c(32,44),
                           "cnt_nmbr"=c(45,47),
                           "dispo_date"=c(48,57),
                           "dispo_code"=c(58,59),
                           "dispo_desc"=c(60,89),
                           "record_type"=c(90,90)), na = c("", "NA"))

lsmci <- read_fwf("Raw_Data/Probation_Assessment_20210311.txt", 
                   fwf_cols("submit_date"=c(1,10), 
                            "xnmbr"=c(23,31), 
                            "assmnt_date"=c(57,66),
                            "total_score"=c(67,68),
                            "criminal_hist"=c(69,70), 
                            "edu_emp"=c(71,72),
                            "fam_marital"=c(73,74),
                            "leisure_rec"=c(75,76),
                            "company"=c(77,78),
                            "alcohol_drug"=c(79,80),
                            "pao"=c(81,82), 
                            "antisocial"=c(83,84)), 
                   col_types=cols("assmnt_date"=col_date(format="%m/%d/%Y")), na = c("", "NA")) 

# 170,068 unique individuals in dispositions 
setDT(disp1)[, .N, by=xnmbr]

#170,239 unique individuals in cases data
setDT(cases1)[, .N, by=xnmbr]

#77,729 unique individuals in lscmi data
setDT(lscmi)[, .N, by=xnmbr]

## 9512 cases that had one disposition only
one_disp <- disp1[, .N, by=xnmbr][N<=1] 

## remove cases that had one disposition only from disp1
# disp1 <- disp1[!(xnmbr %in% one_disp$xnmbr),]

# remove unnecessary cols
disp1 <- disp1[,-(1:3)]
cases1 <- cases1[, -(1:3)]

############################################
### Merge Data 
############################################

# merge dispositions with cases
case_disp <- merge(disp1, cases1, by=c("xnmbr","crt_case_nmbr"))

# convert to date type
cols <- c('case_grant_date', 'sup_close_date', 'dispo_date')
case_disp <- setDT(case_disp)[,  (cols)
                                := lapply(.SD, as.Date, as.POSIXlt, format="%m/%d/%Y"), 
                                .SDcols=cols]

############################################
### Lookup Table
############################################

# Create lookup table for grant type and year combination
grant_year <- expand.grid(Grant_Type=c('FORMAL PROBATION', 'PRC', 'MANDATORY SUPERVISION'), 
                          Year=2011:2020)

# reorder the lookup table
grant_year <- setDT(grant_year)[order(Grant_Type, Year)] 


############################################
### Revocation Codes
############################################

# revocations with remand to custody - nonconditional codes
revoke_code <- c("K3", "K8", "K9","KD", "KG","KH","KP","KL","PJ","T7")


#conditional codes only used when they precede the following:
#UA, UB, UC, UD, UN, P7, PD, CJ, CB, CL, CC, CQ
revoke_cond <- c("UA", "UB", "UC", "UD", "UN", "P7", "PD", "CJ", "CB", 
                 "CL", "CC", "CQ")
revoke_code_cond <- c("K0","K1","K2","K4","K5","KA","KC","KF","KI","KJ",
                      "P0","PR","R2","ZC","ZR")

#get the indices of the rows that meet the condition described above
revoke_index <- case_disp$dispo_code %in% revoke_code_cond & 
  dplyr::lag(case_disp$dispo_code) %in% revoke_cond


############################################
### Calculate Revocations 
############################################
#extract year and month
setDT(case_disp)[, `:=`(year_grant=year(case_grant_date),
                        month_grant=months(case_grant_date, abbreviate=T))]
                 
# function to calculate # of revocations in each year per grant type
num_rev_func <- function(grant_type, year, dat){
  temp <- dat[year_grant == year &                                                # year
               cnt_grant_type_ind == grant_type &                                 # grant type
               (dispo_code %in% revoke_code | revoke_index) &                     # disposition code
               dispo_date <= (case_grant_date %m+% months(36, abbreviate=T)),     # disposition date
             .N, 
             by=.(xnmbr, crt_case_nmbr)] 
  return(sum(temp[,N]))
}

case_disp[
  year_grant == 2012 &                                                # year
    cnt_grant_type_ind == "PRC" &                                 # grant type
    (dispo_code %in% revoke_code | revoke_index) &                     # disposition code
    dispo_date <= (case_grant_date %m+% months(36, abbreviate=T)),     # disposition date
  .N, 
  by=.(xnmbr, crt_case_nmbr)
  ] -> temp

# Calculate the revocations using the lookup table and the function
result_revocation <- map2(.x = grant_year[,1], 
                          .y = grant_year[,2],
                          .f = ~{num_rev_func(.x, .y, case_disp)})

# output the result into a data frame
grant_year_rev <- data.frame(grant_year, Revocations = unlist(result))


############################################
### Calculate Flash Incarcerations 
############################################

# function to calculate # of flash incarcerations in each year per grant type
num_flash_func <- function(grant_type, year, dat){
  temp <- dat[year_grant == year &                                                            # year
               cnt_grant_type_ind == grant_type &                                             # grant type 
               dispo_desc %in% c("FLASH INCARCERATION", "ARRST WRRNT RECAL-FLASH INCARC") &   # disposition code
               dispo_date <= (case_grant_date %m+% months(36, abbreviate=T)),                 # disposition date
             .N, 
             by=.(xnmbr)]
  return(nrow(temp))
}

# Calculate the flash incarcerations using the lookup table and the function
result_flash <- map2(.x = grant_year[,1], 
                     .y = grant_year[,2],
                     .f = ~{num_flash_func(.x, .y)},case_disp2)

# output the result into a data frame
grant_year_flash <- data.frame(grant_year, Flash_Incarceration = unlist(result))



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
