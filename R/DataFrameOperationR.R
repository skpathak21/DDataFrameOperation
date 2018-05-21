#' @title Execute R node and write data to hdfs
#'
#' @description Execute R node and write data to hdfs
#'
#' @param symbol
#'
#' @return
#'
#' @examples PerformLoadData(taskid, sessionid, url, experimentid,hdfsport, hdfsurl, wehdfsport)
#'
#' @export

hdfsurl<-"2000"
HadoopHost <- "35.163.12.29"
HDFSPort <- "9000"
WebHDFSPort <- "50070"
HDFSUserName <- "hduser"

LoadDataFrameFromPath <- function(path,tableschema)
{
  HadoopHost = hdfsurl
  HDFSPort = hdfsport
  WebHDFSPort = wehdfsport
  client = paste0("http://" , HadoopHost , ":" , WebHDFSPort,"/webhdfs/v1")
  hdfspath = paste0("hdfs://" , HadoopHost , ":" , HDFSPort)
  path<-gsub(hdfspath,'',path)
  hdfsUri <- paste0(client, path)
  readParameter <- "&op=LISTSTATUS"
  readCSVParameter <- "&op=OPEN"
  usernameParameter <- paste0("?user.name=",HDFSUserName)
  uri <- paste0(hdfsUri, usernameParameter, readParameter)
  csvlist <- GET(uri)
  csvlist_content <- fromJSON(toJSON(content(csvlist)))
  table_list <- csvlist_content$FileStatuses$FileStatus
  dflist <- list()
  count <- 1
  for (n in 1:nrow(table_list))
  {
    if(table_list$pathSuffix[[n]] != "_SUCCESS" & table_list$pathSuffix[[n]] != "pmml.xml" & table_list$pathSuffix[[n]] != "pmml")
    {
      fileUri = table_list$pathSuffix[[n]]
      csvuri <- paste0(hdfsUri, "/", fileUri, usernameParameter, readCSVParameter)
      data <- read.csv(csvuri, header=FALSE)
      colnames(data) <- tableschema
      dflist[[count]] <- data
    }

  }
  frame <- as.data.frame(dflist)
  return(frame)
}

PerformLoadData <- function(taskid, sessionid, url, experimentid,hdfsport, hdfsurl, wehdfsport) {
  result <- tryCatch({
    TaskId <- list("TaskId"=taskid, "IncludeColumns" = TRUE)
    SessionId <- list("SessionId"=sessionid)
    dturl = paste0(url , '/GetDataSourceSchema')
    body <- list("ParameterValue" = TaskId, "Session" = SessionId)


    hdfsport<-hdfsport
    hdfsurl <- hdfsurl
    wehdfsport <- wehdfsport

    # JSON encoded
    r <- POST(dturl, body = body, encode = "json")
    repo_content <- fromJSON(toJSON(content(r)))
    table_list <- repo_content$ResponseData$TableList
    newlist <- list()
    for (n in 1:nrow(table_list)) {
      path <- table_list$DataLocation[n]
      columnlist <- table_list$ColumnList[n]
      newpath <- path[[1]]
      columnlists <- list()
      for (c in 1:nrow(columnlist[[1]])) {
        columnlists[[c]] = columnlist[[1]]$MappedAliasName[[c]]
      }
      #customSchema <- do.call(structType, columnlists)
      if(newpath != '')
      {
        datafrm <- LoadDataFrameFromPath(newpath, columnlists)
        tablelocpath <- table_list$TABLE_NAME[n]
        newtablelocpath <- tablelocpath[[1]]
        newlist[[newtablelocpath]]=datafrm
      }

    }
    updatedlist <- LoadOtherData(taskid, sessionid, url, sc, experimentid, newlist)
    return(updatedlist)
  }, error = function(err) {

    # error handler picks up where error was generated
    print(paste("Unexpected error:  ",err))
  })
}
LoadOtherData <- function(taskid, sessionid, url, sc, experimentid, newlist) {
  TaskId <- list("TaskId"=taskid)
  ExperimentId <- list("ExperimentId"=experimentid)
  SessionId <- list("SessionId"=sessionid)
  jsondata <- list("ParameterValue" = ExperimentId, "Session" = SessionId)
  rdata <- list("ParameterValue" = TaskId, "Session" = SessionId)
  urlExp = paste0(url , '/GetExperiments')
  rExp <- POST(urlExp, body = rdata, encode = "json")
  resultExp <- content(rExp)
  nodeids <- list()
  nodeidsdict <- list()
  nodecaptioncount = 1
  #print(resultExp)
  if(resultExp != '' && resultExp$ResponseData != '' && resultExp$ResponseData[[1]]$GraphJSON != '')
  {
    l <- fromJSON(resultExp$ResponseData[[1]]$GraphJSON)

    count <- 1
    for (n in 1:length(l$cells$type)) {
      if(l$cells$type[n] == 'devs.Model')
      {
        nodeids[count]=l$cells$id[n]
        nodeidsdict[[l$cells$id[n]]]=l$cells$NodeCaption[n]
        count <- count + 1
      }

    }
  }
  ExpById = paste0(url , '/GetExperimentByExperimentID')
  rsExpById <- POST(ExpById, body = jsondata, encode = "json")
  resultExpById <- content(rsExpById)
  if(resultExpById != '' && resultExpById$ResponseData != '' && resultExpById$ResponseData$ResponseData != '' && resultExpById$ResponseData$ResponseData$JobsStatus != '')
  {
    Jobstatus <- fromJSON(toJSON(resultExpById$ResponseData$ResponseData$JobsStatus))
    frmjobstatus <- as.data.frame(Jobstatus)
    for (n in 1:length(nodeids)) {
      #print(n)
      nodeid <- nodeids[[n]]
      #print(nodeid)
      bcd <- frmjobstatus[which(frmjobstatus$Status == 2 & frmjobstatus$JobNodeID == nodeid & frmjobstatus$JobNodeID != "0" ), ]
      dtlocationlength <- length(bcd$DataLocation)
      #print(frmjobstatus)
      if(dtlocationlength > 0)
      {
        dtloc <- fromJSON(toJSON(bcd$DataLocation[dtlocationlength]))
        dtlocrep<-gsub("'",'"',dtloc)
        dtloclist <- fromJSON(dtlocrep)
        #print(dtloclist)
        if(!is.null(dtloclist$SplitData))
        {
          if(dtloclist$MetaData != '')
          {
            metadata <- strsplit(dtloclist$MetaData, ",")
            metadatatypees <- strsplit(dtloclist$MetaDataType, ",")
            columnlists <- list()
            for (c in 1:length(metadata[[1]])) {
              columnlists[[c]] = metadata[[1]][c]
            }
            #customSchema1 <- do.call(structType, columnlists)
            splitlocation <- dtloclist$SplitData["DataLocation"]
            for (n in 1:nrow(splitlocation)) {
              #print(splitlocation[[1]][n])
              if(splitlocation[[1]][n] != '')
              {
                newpath <- splitlocation[[1]][n]
                #datafrm <- read.df(newpath, "csv", customSchema1)
                datafrm <- LoadDataFrameFromPath(newpath, columnlists)
                newtablelocpath <- paste0(nodeidsdict[[nodeid]],'_',as.character(nodecaptioncount))
                newlist[[newtablelocpath]]=datafrm
                nodecaptioncount = nodecaptioncount + 1
              }
            }
          }
        }
        else
        {

          #print(bc)
          #print(dtloclist$DataLocation)
          if(dtloclist$DataLocation != '')
          {
            if(dtloclist$MetaData != '')
            {
              metadata <- strsplit(dtloclist$MetaData, ",")
              metadatatypees <- strsplit(dtloclist$MetaDataType, ",")
              columnlists <- list()
              for (c in 1:length(metadata[[1]])) {
                columnlists[[c]] = metadata[[1]][c]
              }
              #customSchema2 <- do.call(structType, columnlists)
              newpath <- dtloclist$DataLocation
              datafrm <- LoadDataFrameFromPath(newpath, columnlists)
              newtablelocpath <- paste0(nodeidsdict[[nodeid]],'_',as.character(nodecaptioncount))
              newlist[[newtablelocpath]]=datafrm
              nodecaptioncount = nodecaptioncount + 1
            }
          }
        }

      }

    }
  }
  #print(GlobalInputXml)
  return(newlist)
  #print(nodeids)
  #print(nodeidsdict)
  #print(resultExpById)
}
UpdateJob <-  function(XMLData, DataLocation, status){
  xmldoc <- read_xml(XMLData)
  UUID <- xml_text(xml_find_all(xmldoc, ".//UUID"))
  ExperimentId <- xml_text(xml_find_all(xmldoc, ".//ExperimentId"))
  # AppKey = str(xmldoc.getElementsByTagName("AppKey")[0].firstChild.nodeValue)
  NodeId <- xml_text(xml_find_all(xmldoc, ".//NodeId"))
  ServiceUrl <- xml_text(xml_find_all(xmldoc, ".//ServiceUrl"))
  ServiceUrl = paste0(ServiceUrl, "/Updatejob")
  datalocation <- gsub("`", '\"', DataLocation)
  jobid <- paste0("application_" , UUID)
  body <- list("DataLocation" = datalocation, "ExperimentID" = ExperimentId, "JobID"= jobid,
               "AppKey"= "This1Is2Temporary3Application4Key5For6Testing7",
               "Message"= "Job Has Completed",
               "JobNodeID"= NodeId,
               "Status"= status,
               "UUID"= UUID)
  r <- POST(ServiceUrl, body = body, encode = "json")
  repo_content <- fromJSON(toJSON(content(r)))
  print(repo_content)
  return(repo_content)
}
FinalOutput <- function(df) {

  HadoopHost = hdfsurl
  HDFSPort = hdfsport
  WebHDFSPort = wehdfsport

  uniqueId <- UUIDgenerate()
  columnname <- colnames(df)
  columnstring <- paste(columnname, collapse=", ")
  MetaDataRes <- columnstring[1]

  dtypecolumn <- sapply(dfset$DiagnosticData, typeof)
  #columndatatype <- c(dtypecolumn)
  columndatatype <- list()
  count <- 1
  for (n in sapply(dfset$DiagnosticData, typeof)) {
    columndatatype[[count]] = n[1]
    count <- count + 1
  }
  columndatatypestring <- paste(columndatatype, collapse=", ")
  MetaDataTypeRes <- columndatatypestring[1]

  #HdfsFilePath <- "hdfs://ec2-34-217-105-188.us-west-2.compute.amazonaws.com:9000/tmp/"
  #writeFilePath <- paste0(HdfsFilePath, uniqueId)
  #write.df(df, path=writeFilePath, source="csv", mode="overwrite")
  ### Parameters to set
  # WebHDFS url
  client = paste0("http://" , HadoopHost , ":" , WebHDFSPort,"/webhdfs/v1")
  #hdfsUri <- "http://ec2-34-217-105-188.us-west-2.compute.amazonaws.com:9870/webhdfs/v1"
  # Url where you want to append the file
  #fileUri <- "/user/hive/myfile.csv"
  writeFolderPath <- paste0("/user/pangea/dev/" , uniqueId , "/model")
  writeFilePath <- paste0(writeFolderPath , "/" , uniqueId , ".csv")
  # Optional parameter, with the format &name1=value1&name2=value2
  optionnalParameters <- "&overwrite=true"

  # CREATE => creation of a file
  writeParameter <- "?op=CREATE"

  # Concatenate all the parameters into one uri
  uri <- paste0(client, writeFilePath, writeParameter, optionnalParameters)

  # Ask the namenode on which datanode to write the file
  response <- PUT(uri)

  # Get the url of the datanode returned by hdfs
  uriWrite <- response$url
  #print(uriWrite)
  # The data you want to upload
  # If your data is a file on your disk, leave it that way
  data <- df
  tmpFile <- paste0(uniqueId , ".csv")
  # Write a temporary file on the disk
  if(!file.exists(tmpFile)) {
    write.table(data, file = tmpFile, row.names=FALSE, col.names=FALSE)

    # Upload the file with a PUT request
    responseWrite <- PUT(uriWrite, body = upload_file(tmpFile))

    # removes the temporary file
    file.remove(tmpFile)
  } else {
    stop("A file named 'tmp.csv' already exists in the current directory")
  }
  Datalocation <- paste0("hdfs://", HadoopHost, ":", HDFSPort, writeFolderPath)
  ModelLocation<-paste0("hdfs://" , HadoopHost, ":", HDFSPort, writeFolderPath)
  PMMLLocation<-paste0("hdfs://", HadoopHost, ":", HDFSPort, writeFolderPath, "/pmml")
  resultlist = list(MetaData=MetaDataRes, MetaDataType=MetaDataTypeRes, DataLocation=Datalocation,ModelLocation=ModelLocation,PMMLLocation=PMMLLocation)
  resultJson <- toJSON(resultlist)
  resultfinal <- gsub('\"', "`", resultJson)
  resultfinal <- gsub("\\[|\\]", "", resultfinal)
  pos = grep('MetaData', resultfinal)
  IsSuccess <- FALSE
  if(length(pos) > 0)
  {
    IsSuccess <- TRUE
    print("==================================")
    print(resultfinal)
    print("==================================")
  }
  if (GlobalInputXml != '')
  {
    if(IsSuccess == TRUE)
    {
      print("Success...")
      UpdateJob(GlobalInputXml, resultfinal, 2)
      print("=============================Save PMML ")
      SaveFileToHdfs(writeFolderPath,MetaDataTypeRes,MetaDataRes,GlobalInputXml)
    }
    else
    {
      UpdateJob(GlobalInputXml, returndata, 3)
      print("completed python script execution with error...")
    }
  }
  #print(resultfinal)
  return(resultfinal)
}
GeneratePmml <- function(colnames, coltypes, script)
{
  xmldoc <-script
  # read_xml(
  #   "<Action Id='6254f8f0-73d2-48e0-ad0d-f4b804f90513' NextId='' PreviousId=''><Type>Script</Type><Name>Python</Name><Property Name='SelectedColumns'>Duration_ms</Property><Property Name='PredictedName'>test</Property><Property Name='PredictedDataType'>String</Property><Property Name='ApplyTransformationTo'><![CDATA[exec ('from DataFrameOperation import DataframeOpr \ndfs = DataframeOpr.PerformLoadData('', 5,'6121a56cb86f45998c0ede3940ff30c7_1', 'http://localhost:56620/RESTService.svc/','9F9AAAA2-5398-4557-9D0E-8CD85203093D')DataframeOpr.FinalOutput(dfs['DiagnosticData'])')]]></Property><UUID>ec095401a6784140b0158707d9cddd3f</UUID><ExperimentId>9F9AAAA2-5398-4557-9D0E-8CD85203093D</ExperimentId><ServiceUrl>http://34.216.98.159:8085/MTS/RESTService.svc</ServiceUrl><NodeId>6254f8f0-73d2-48e0-ad0d-f4b804f90513</NodeId></Action>"
  # )
  Properties <- xml_text(xml_find_all(xmldoc, ".//Property"))
  selectedcolumns <- Properties[1]
  predictedname <- Properties[2]
  predicteddatatype <- Properties[3]
  execscript <- Properties[4]
  # execscriptContent <-
  #   strsplit(execscript, "(\\r\\n?+|.\\n)", perl = TRUE)[[1]]
  #
  # for (execscriptContentItem in execscriptContent)
  # {
  #   if ("DataframeOpr" == execscriptContentItem ||
  #       'dfs["' == execscriptContentItem)
  #   {
  #     execscript <- execscript.replace(execscriptContentItem, "")
  #   }
  # }
  execscript<-trimws(execscript, "l")
  #trimws(execscript, "r")
  PmmlTag(colnames)
  fullschema <- ''
  fullschema <- paste0(fullschema, startpmmltag)
  fullschema <-
    paste0(fullschema, CreateHeader('python script', 'Pangea'))
  print(fullschema)
  fullschema <- paste0(fullschema, starttranformationtag)
  fullschema <-
    paste0(
      fullschema,
      CreateDerivedFunction(
        funcname = 'pangeacommand.PythonEngine.ScriptExecutor.execute',
        datatype = 'float',
        optype = 'continuous',
        paramname = 'params',
        paramoptype = 'continuous',
        paramdatatype = 'float'

      )
    )
  print(fullschema)
  fullschema <-
    paste0(
      fullschema,
      SetDerivedField("pangeacommand.PythonEngine.ScriptExecutor.execute",
                      selectedcolumns,
                      predictedname,
                      predicteddatatype,
                      paste0("<![CDATA['" ,
                             execscript ,
                             "']]>")
      )
    )
  fullschema <- paste0(fullschema, endtranformationtag)
  fullschema <- paste0(fullschema, endpmmltag)
  print(fullschema)
  return(fullschema)

}

SaveFileToHdfs <- function(writeFolderPath,
                           keys,
                           values,
                           inputscript)
{
  print("start function SaveFileToHdfs")
  HadoopHost <- hdfsurl
  HDFSPort <- hdfsport
  WebHDFSPort <- wehdfsport

  client_hdfs <- paste0("http://" , HadoopHost , ":" , WebHDFSPort, "/webhdfs/v1")

  pmmlxml<-GeneratePmml(keys, values, inputscript)
  print(pmmlxml)
  filename <- paste0(writeFolderPath , "/pmml/" , "part-00000")
  print("============File name for PMML==========")
  print(filename)

  optionnalParameters <- "&overwrite=true"

  # CREATE => creation of a file
  writeParameter <- "?op=CREATE"

  # Concatenate all the parameters into one uri
  uri <-
    paste0(client_hdfs, filename, writeParameter, optionnalParameters)
  print(uri)
  # Ask the namenode on which datanode to write the file
  response <- PUT(uri)
  uniqueId <- UUIDgenerate()
  # Get the url of the datanode returned by hdfs
  uriWrite <- response$url
  print(uriWrite)
  #tmpFile <- paste0(uniqueId , ".csv")
  tmpFile <- paste0(uniqueId)
  # Write a temporary file on the disk
  if(!file.exists(tmpFile)) {
    write.table(pmmlxml, file = tmpFile, row.names=FALSE, col.names=FALSE)

    # Upload the file with a PUT request
    responseWrite <- PUT(uriWrite, body = upload_file(tmpFile))
    print(responseWrite)
    # removes the temporary file
    file.remove(tmpFile)
  } else {
    stop("A file temp file already exists with same name in the current directory")
  }
  print("end function SaveFileToHdfs")
}
PmmlTag <- function(columns)
{
  startdictionarytag <<- ''
  enddictionarytag <<- ''
  startpmmltag <<- ''
  starttranformationtag <<- ''
  endtranformationtag <<- ''
  startpmmltag <<-'<?xml version="1.0" encoding="UTF-8"?><PMML xmlns="http://www.dmg.org/PMML-4_2" version="4.2">'
  endpmmltag <<- ''
  endpmmltag <<- '</PMML>'
  starttranformationtag <<- '<TransformationDictionary>'
  endtranformationtag <<- '</TransformationDictionary>'
  if ("" != columns)
  {
    splitresult <- strsplit(columns, ',')[[1]]
    startdictionarytag <<-paste0('<DataDictionary numberOfFields="' ,toString(length(splitresult)) ,'">')
    enddictionarytag <<- '</DataDictionary>'
  }
}

CreateHeader <- function(description, appname)
{
  print("Header print")
  headertext <-
    paste0(
      '<Header description="',
      description,
      '"><Application name="',
      appname ,
      '"/><Timestamp>' ,
      toString(Sys.time()) ,
      '</Timestamp></Header>'
    )
  print(headertext)
  return(headertext)
}

CreateDerivedFunction <-
  function(funcname,
           datatype,
           optype,
           paramname,
           paramoptype,
           paramdatatype)
  {
    print("Step3")
    strvalue = ''
    definestarttag <-
      paste0(
        '<DefineFunction name="',
        funcname,
        '" dataType="',
        datatype ,
        '" optype="',
        optype ,
        '">'
      )
    paramfieldtag <-
      paste0(
        '<ParameterField name="',
        paramname ,
        '" optype="' ,
        paramoptype ,
        '" dataType="' ,
        paramdatatype ,
        '"/>'
      )
    discretizestarttag <-
      paste0(
        '<Discretize field="' ,
        paramname ,
        '" ',
        'defaultValue="args,argTypes,argValues">',
        '<DiscretizeBin binValue="df,script,outparam">' ,
        '<Interval closure="openClosed"/>' ,
        '</DiscretizeBin><DiscretizeBin binValue="string,PythonCode,string">',
        '<Interval closure="openClosed"/>',
        '</DiscretizeBin></Discretize>'
      )
    defineendtag <- '</DefineFunction>'
    strvalue = paste(strvalue, definestarttag)
    strvalue = paste(strvalue, paramfieldtag)
    strvalue = paste(strvalue, discretizestarttag)
    strvalue = paste(strvalue, defineendtag)
    print("Step4")
    print(strvalue)
    return(strvalue)
  }

SetDerivedField <-
  function(funcname,
           selectedcolumns,
           predictedname,
           predicteddatatype,cdata)
  {
    print(selectedcolumns)
    print(predictedname)
    print(predicteddatatype)
    datafield = ''
    if ('' != selectedcolumns &&
        '' != predictedname && '' != predicteddatatype)
    {
      datafield <-
        paste0(
          datafield,
          '<DerivedField dataType="' ,
          predicteddatatype ,
          '" name="',
          predictedname,
          '" optype="continuous">'
        )
      datafield <-
        paste0(datafield,
               CreateApplyFunc(funcname, selectedcolumns, cdata))
      datafield <- paste0(datafield, "</DerivedField>")
      return(datafield)
    }
  }

CreateApplyFunc <- function(funcname, fieldnames, cdata)
{
  applystarttag <- paste0("<Apply function='",funcname , "'>")
  applyendtag <- "</Apply>"
  extentionstart <- paste0("<Extension><Script>",cdata , "</Script></Extension>")
  colnameslst <- strsplit(fieldnames, ',')[[1]]
  fieldsstr = ''
  if (length(colnameslst) > 0)
  {
    for (index in seq_along(colnameslst)) {
      fieldsstr <-
        paste0(fieldsstr, "<FieldRef field='" , colnameslst[index], "'/>")
      applystarttag <- paste0(applystarttag, extentionstart)
      applystarttag <- paste0(applystarttag, fieldsstr)
      applystarttag <- paste0(applystarttag, applyendtag)

      return(applystarttag)
    }

  }
}
