##########################################################################################################################################

##########################################################################################################################################

#connection_string <- "Driver=SQL Server;Server=localhost; Database=Hospital;UID=hzmarrou;PWD=trefoil1234"
connection_string <- "Driver=SQL Server;Server=TREFOILML;Database=Hospital;UID=hzmarrou;PWD=trefoil1234"
sql   <-   RxInSqlServer(connectionString = connection_string)
local <- RxLocalSeq()
