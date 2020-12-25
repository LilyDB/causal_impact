# Databricks notebook source
# MAGIC %md # 
# MAGIC 
# MAGIC #### This analysis uses 2 data sets
# MAGIC ##### 1. Daily transactions: 
# MAGIC ###### Aim: To detect trend, seasonality in the time series data
# MAGIC 
# MAGIC Query:
# MAGIC       
# MAGIC       SELECT o.parent_vertical,
# MAGIC             p.created_at,
# MAGIC             sum(no_transactions) no_transactions
# MAGIC 
# MAGIC       from `gc-prd-ext-data-test-prod-906c.gc_paysvc_live.organisations` o 
# MAGIC       inner join `gc-prd-ext-data-test-prod-906c.gc_paysvc_live.mandates` m
# MAGIC       on o.id = m.organisation_id
# MAGIC       left join 
# MAGIC                 (select mandate_id, 
# MAGIC                         cast(created_at as date) as created_at,
# MAGIC                         count(id) no_transactions 
# MAGIC                  from `gc-prd-ext-data-test-prod-906c.gc_paysvc_live.payments` p
# MAGIC                  where 
# MAGIC                      amount > 0  and created_at is not null and charge_date is not null --successfully transaction
# MAGIC                  group by mandate_id,currency,cast(created_at as date)  ) p
# MAGIC       on p.mandate_id = m.id
# MAGIC 
# MAGIC       where p.created_at is not null
# MAGIC 
# MAGIC       group by o.parent_vertical,  p.created_at
# MAGIC 
# MAGIC 
# MAGIC ##### 2. Cummulative number of activations: 
# MAGIC ###### Aim: To compare pre and post campaign 
# MAGIC 
# MAGIC 
# MAGIC Query:
# MAGIC 
# MAGIC     select 
# MAGIC         distinct t1.created_at, 
# MAGIC         sum(count(t1.mandate_id)) over ( order by t1.created_at asc) as cum_activated_mandates,
# MAGIC         case when t1.created_at < cast('2016-12-01' as date) 
# MAGIC         then 0 else 1
# MAGIC         end as campaign_status  ---0 pre, 1 post
# MAGIC     from 
# MAGIC     (
# MAGIC               select 
# MAGIC                    min(cast(p.created_at as date)) as created_at,   
# MAGIC                    mandate_id
# MAGIC 
# MAGIC               from `gc-prd-ext-data-test-prod-906c.gc_paysvc_live.payments` p
# MAGIC               inner join  `gc-prd-ext-data-test-prod-906c.gc_paysvc_live.mandates` m
# MAGIC               on m.id = p.mandate_id
# MAGIC 
# MAGIC               where p.created_at is not null
# MAGIC               and p.amount > 0 and p.created_at is not null and p.charge_date is not null --successfully transaction
# MAGIC               group by mandate_id
# MAGIC             ) 
# MAGIC             as t1
# MAGIC 
# MAGIC     group by t1.created_at

# COMMAND ----------

install.packages(c("CausalImpact",
                   "ggfortify",
                    "changepoint",
                    "strucchange",
                    "ggpmisc",
                    "ggeasy")
                )

# COMMAND ----------

library(CausalImpact)
library(tidyverse)
library(xts)
library(zoo)
library(lubridate)
library(SparkR)
library(tidyr)
library(dplyr)
library(ggeasy)
require(gridExtra)

# COMMAND ----------

# Cummulative activation rate data set
act <- read.df("dbfs:/FileStore/shared_uploads/phuong.le@man-es.com/CausualImpact_all.csv",
                "csv",
                header=TRUE)

# Daily transaction data set
daily <- read.df("dbfs:/FileStore/shared_uploads/phuong.le@man-es.com/dailytransaction.csv", 
                 "csv", 
                 header=TRUE)
act <- collect(act)
daily <- collect(daily)

# COMMAND ----------

# Explore data
daily$no_transactions <-as.numeric(daily$no_transactions)
daily$created_at<-as.Date(daily$created_at)
str(daily)

# COMMAND ----------

transaction_plot <- ggplot(data = daily, aes(created_at, 
                         no_transactions)) +
                          geom_line(color = "lightblue", size = 0.5) +
                          geom_point(color = "red", size = 0.05) + 
                          labs(title = "Number of transactions on daily basis",
                          y = "Transactions", x = "") + 
                          facet_grid( parent_vertical ~.,scales="free") 
transaction_plot 

# COMMAND ----------

# MAGIC %md # 
# MAGIC 
# MAGIC ##### Comments about the plot above:
# MAGIC 
# MAGIC ######High growth in number of transactions pre vs post campaign can be seen in:
# MAGIC     Health care
# MAGIC     Sport fitness
# MAGIC     Tradesmen
# MAGIC     Digital service
# MAGIC ######Low growth or no growth at all, indicating campaign seems not to influence much in: 
# MAGIC     Prefessional and financial service
# MAGIC     Societies and club
# MAGIC     Property
# MAGIC     
# MAGIC ######Except sociaties and club, all other verticals have seasonal impact over time.
# MAGIC ######Seasonality can be seen in 
# MAGIC 1. Monthly season
# MAGIC     Professional and financial services 
# MAGIC     Sport fitness
# MAGIC 2. Combined monthly and weekly season
# MAGIC     Property
# MAGIC     Digital services
# MAGIC     Tradesmen and non professionals services
# MAGIC 3. Weekly season
# MAGIC     Healthcare
# MAGIC 
# MAGIC => These observations will be used to highlight seasonality in the times series model in later part. 
# MAGIC 
# MAGIC ##### Causal Impact model
# MAGIC 
# MAGIC This implementation use causal model using Bayesian structural time-series model
# MAGIC 
# MAGIC Use 2 libraries bsts and CasualImpact to measure the causal effect of the campaign on a time series.
# MAGIC 
# MAGIC Source:
# MAGIC 
# MAGIC   https://cran.r-project.org/web/packages/CausalImpact
# MAGIC   

# COMMAND ----------

# Explore cummulative activation rate dataset
vars <-c("created_at","cum_activated_mandates","campaign_status")
pre.period <- c(as.Date("2016-04-02"), as.Date("2016-11-30"))
post.period <- c(as.Date("2016-12-01"),as.Date("2017-03-06"))

# COMMAND ----------

act$created_at <- as.Date(act$created_at)
act$cum_activated_mandates <- as.numeric(act$cum_activated_mandates)
act$campaign_status  <- as.numeric(act$campaign_status)
str(act)

# COMMAND ----------


cum_activated_mandates<-xts(as.numeric(act$cum_activated_mandates),
                            as.Date(act$created_at))

campaign_status<-xts(as.numeric(act$campaign_status),
                     as.Date(act$created_at))

act_ts <- cbind(cum_activated_mandates,campaign_status)
act_ts <-zoo(act_ts)

head(act_ts)

# COMMAND ----------

# Fit with month seasonal
impact4 <- CausalImpact(act_ts, 
                          pre.period, 
                          post.period,
                          model.args = list(niter = 1000,
                                            nseasons = 365, 
                                            season.duration =7))
plot(impact4)

# COMMAND ----------

summary(impact3)
