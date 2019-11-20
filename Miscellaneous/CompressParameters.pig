/*
** Pig Script: Compress HDFS data
**
** Purpose:
** Compress HDFS data while keeping the original folder structure
**
** Paramater
** $date - in the format YYYYMMDD (following HDFS folder structure)
**
** Example call:
** pig Compressparameters.pig
*/

-- set compression
set output.compression.enabled true;
set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

-- set large split size to merge small files together and then compress
set pig.maxCombinedSplitSize 2684354560;

-- load files and store them again (using compression codec)
inputFiles = LOAD '/user/oracle/SkyNet/HiPass2B/parameters' using PigStorage();
STORE inputFiles INTO '/user/oracle/SkyNet/Temp/' USING PigStorage();

-- remove original folder and rename gzip folder to original
-- remove the $date folder, rename the $date_gz to $date
 rm hdfs://bigdatalite.localdomain:8020/user/oracle/SkyNet/HiPass2B/parameters
 mv hdfs://bigdatalite.localdomain:8020/user/oracle/SkyNet/Temp hdfs://bigdatalite.localdomain:8020/user/oracle/SkyNet/HiPass2B/parameters
