connectDBI <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- try(
            do.call(DBI::dbConnect, args),
            silent = TRUE
        )
    if(inherits(con, 'try-error')) return(NULL)
    return(con)
}

bioDBRadar <- function(dauth){
    f <- file.path(dauth, 'auth', 'biodb.con')
    if(!file.exists(f)) return(NULL)
    # drv <- RMySQL::MySQL()
    drv <- RPostgres::Postgres()
    con <- connectDBI(readRDS(f)$connection, drv)
    if(is.null(con)){
        Sys.sleep(3)
        con <- connectDBI(readRDS(f)$connection, drv)
        if(is.null(con)) return(NULL)
    }
    return(con)
}

closeDB <- function(con){
    if(!is.null(con)){
        DBI::dbDisconnect(con)
    }
}

tableExists <- function(con, table_name) {
  query <- sprintf("SELECT EXISTS (
                      SELECT FROM information_schema.tables
                      WHERE table_schema = 'public'
                        AND table_name = '%s'
                    );", table_name)
  res <- DBI::dbGetQuery(con, query)
  return(isTRUE(res$exists))
}

createDBTable <- function(con, table_name, cascade, ...){
    args <- list(...)
    if(length(args) == 0)
        stop('No create_definition args found')
    args <- do.call(c, args)
    args <- paste0(args, collapse = ', ')
    args <- paste0('(', args, ')')

    statement <- paste(
        "DROP TABLE IF EXISTS", table_name
    )
    if(cascade){
        statement <- paste(statement, "CASCADE")
    }
    DBI::dbExecute(con, statement)
    statement <- paste(
        "CREATE TABLE", table_name, args
    )
    DBI::dbExecute(con, statement)

    return(0)
}
