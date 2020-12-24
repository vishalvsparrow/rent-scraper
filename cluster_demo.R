rm(list = ls(all.names = TRUE))
data("USArrests")
ss <- sample(1:50, 20)
ss
df <- USArrests
df <- scale(USArrests[ss, ])
df

library(e1071)
cm <- cmeans(df, 4)
head(cm$membership)

library(corrplot)
corrplot(cm$membership, is.corr = FALSE)

library(factoextra)
fviz_cluster(list(
  
  data = df, cluster = cm$cluster),
  ellipse.type = "norm",
  ellipse.level = 0.68,
  palette = "jco",
  ggtheme =theme_minimal()
  
  
)


## Bayside clustering

#install.packages('bigrquery')
#install.packages('dplyr')
library(bigrquery)
library(DBI)
library(dplyr)


con <- dbConnect(
  bigrquery::bigquery(),
  project = "vishal-covid-tracker",
  dataset = "rent_tracker",
  billing = "vishal-covid-tracker"
)


dbListTables(con)

sql = 'select * from clean_view'

df <- dbGetQuery(con, sql)

df_units <- df[which(df$is_unit == 1),]

# get unit floor
#df_units$unitFloor <- lapply(df_units$unitNumber, FUN = function(x) substr(x, 1, 1))

tmp_df <- df_units %>% 
  select(request_d_clean, unitBuildingNumber, 
                           planName, unitNumber, planBathrooms, unitMinPrice,
                           planBedrooms, unitFloor, planSquareFeet) %>% 
  group_by(unitBuildingNumber, unitNumber) %>% 
  arrange(desc(request_d_clean)) %>% 
  mutate(m_row = row_number()) %>% 
  filter(m_row == 1) 

# correlation
library(corrplot)
library("Hmisc")

tmp_df$unitBuildingNumber <- as.numeric(tmp_df$unitBuildingNumber)
tmp_df$planBathrooms <- as.numeric(tmp_df$planBathrooms)
tmp_df$planBedrooms <- as.numeric(tmp_df$planBedrooms)
tmp_df$unitFloor <- as.numeric(tmp_df$unitFloor)

res <- cor(tmp_df[, c(2,5,6,7, 8)])           
corrplot(res)

# nothing found in correlation. Try getting group statistics

tmp <- tmp_df %>%
  mutate(id = paste(unitBuildingNumber, unitNumber, sep = '-')) %>%
  group_by(id, unitNumber, unitBuildingNumber, planBathrooms, planBedrooms, unitFloor) %>%
  summarise(price = median(unitMinPrice))

m_id <- tmp$id

tmp <- tmp[-c(1, 2)]

row.names(tmp) <- m_id

## try clustering

install.packages('ggdendro')
library(ggdendro())

hc = hclust(dist(tmp), "average")
#hc = hclust(dist(dat_copy))
ggdendrogram(hc, rotate = FALSE, size = 2)

plot(hc)
hc

# ?hclust
# (dist(dat[,c(1,2,3,4,5,6)]))

clusterCut <- cutree(hc, k= 4)

tmp$id <- row.names(tmp)

tmp$cluster <- clusterCut

#class(clusterCut)
#plot((tmp$id), clusterCut)
#plot(clusterCut)

tmp %>% group_by(cluster) %>% summarise(median_price = median(price))

## nothing in cluster

# look at median prices by building numbers

t <- tmp_df %>%  
  mutate(price_sq_feet = unitMinPrice/planSquareFeet, planBedBath = paste(planBedrooms, planBathrooms)) %>% 
  group_by(unitBuildingNumber, planSquareFeet, unitFloor, planBedBath) %>%
  summarise(mean_price_sq_feet = mean(price_sq_feet))
write.csv(t, 'tmp.csv')
