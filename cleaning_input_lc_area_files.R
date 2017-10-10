
setwd("C:/Data/lshift_all/outputs/raw")

#seleect only columns of interest and rename to allwo easy import in postgis


for (file in list.files(pattern="*csv")){
  df<-read.csv(file, header=TRUE)
  df<-data.frame(cbind(df$cell_id,df$GRIDCODE,df$POLY_AREA*1000000))
  names(df)<-c("cell_id","lc","area_lc_cell")
  write.csv(df,file.path(getwd(),"edits",file), row.names=FALSE)
}