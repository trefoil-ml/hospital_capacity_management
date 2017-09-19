##########################################################################################################################################

##########################################################################################################################################

#connection_string <- "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"
connection_string <- "Driver=SQL Server;Server=localhost;Database=Hospital4;UID=hzmarrou;PWD=trefoil1234"

sql <-   RxInSqlServer(connectionString = connection_string)
local <- RxLocalSeq()
