#' Vertical profile for MeteoRwanda radar data, production mode.
#'
#' Compute a vertical profile of bird and insect
#' from MeteoRwanda new format \code{ODIM HDF5}
#' and store the outputs into \code{BioDBRadar}.
#' 
#' @param bioradar_dir character, full path to the parent folder containing \code{BioConfigRadar}.
#' @param radar_id integer, the radar id.
#'  
#' @export

production_rwanda_vp <- function(bioradar_dir, radar_id = 1){
    Sys.setenv(TZ = 'UTC')
    config_dir <- file.path(bioradar_dir, 'BioConfigRadar')
    dir_config <- file.path(config_dir, 'config')
    log_file <- get_log_file(bioradar_dir, 'logs_vp', 'vp')

    yaml_file <- file.path(dir_config, 'config_datasets.yaml')
    if(!file.exists(yaml_file)){
        msg <- paste('File does not exist:', yaml_file)
        format_out_msg(msg, log_file)
        return(-1)
    }
    radar_info <- yaml::read_yaml(yaml_file)

    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        msg <- 'Unable to connect to BioDBRadar.'
        format_out_msg(msg, log_file)
        return(-1)
    }
    sqlCmd <- sprintf(
        "SELECT end_time FROM vp_timerange WHERE radar_id=%s",
        radar_id
    )
    last <- DBI::dbGetQuery(con, sqlCmd)
    closeDB(con)

    start_time <- format(
        last$end_time + 1, '%Y-%m-%d %H:%M:%S'
    )
    now <- Sys.time()
    end_time <- format(now, '%Y-%m-%d %H:%M:%S')

    radar_files <- get_data_files_list(
        radar_info$radar$polar, start_time, end_time
    )

    if(is.null(radar_files)){
        return(0)
    }

    radar_files <- lapply(radar_files, function(f){
        file.path(f$dir, f$files)
    })
    radar_files <- do.call(c, radar_files)
    radar_files <- file.path(
        radar_info$radar$polar$dir, radar_files
    )

    klust <- parallel::makeCluster(5)
    doSNOW::registerDoSNOW(klust)
    if(length(radar_files) >= 5){
        `%dopar%` <- foreach::`%dopar%`
    }else{
        `%dopar%` <- foreach::`%do%`
    }

    ret <- foreach::foreach(
            jlp = seq_along(radar_files)
        ) %dopar% {
        ret <- try(
            wrapper_rwanda_vp(
                radar_files[jlp], bioradar_dir, radar_id
            ),
            silent = TRUE
        )
        if(inherits(ret, 'try-error')){
            msg1 <- paste('File:', radar_files[jlp], '\n')
            msg2 <- gsub('[\r\n]', '', ret[1])
            format_out_msg(paste0(msg1, msg2), log_file)
            return(-1)
        }
        return(0)
    }

    parallel::stopCluster(klust)

    return(0)
}

#' Vertical profile for MeteoRwanda radar data, one file.
#'
#' Compute a vertical profile of bird and insect
#' from MeteoRwanda new format \code{ODIM HDF5}
#' and store the outputs into \code{BioDBRadar}.
#' 
#' @param bioradar_dir character, full path to the parent folder containing \code{BioConfigRadar}.
#' @param time character, the time to process, format 'yyyy-mm-dd hh:mm:ss'.
#' @param radar_id integer, the radar id.
#'  
#' @export

process_rwanda_vp <- function(bioradar_dir, time, radar_id = 1){
    config_dir <- file.path(bioradar_dir, 'BioConfigRadar')
    dir_config <- file.path(config_dir, 'config')
    log_file <- get_log_file(bioradar_dir, 'logs_vp', 'vp')

    yaml_file <- file.path(dir_config, 'config_datasets.yaml')
    if(!file.exists(yaml_file)){
        msg <- paste('File does not exist:', yaml_file)
        format_out_msg(msg, log_file)
        return(-1)
    }
    radar_info <- yaml::read_yaml(yaml_file)
    radar_file <- get_data_file_path(
        radar_info$radar$polar, time
    )
    if(is.null(radar_file)){
        msg <- paste('No file found for time close to', time)
        format_out_msg(msg, log_file)
        return(-1)
    }

    ret <- try(
        wrapper_rwanda_vp(
            radar_file, bioradar_dir, radar_id
        ),
        silent = TRUE
    )
    if(inherits(ret, 'try-error')){
        msg1 <- paste('File', radar_file, '\n')
        msg2 <- gsub('[\r\n]', '', ret[1])
        format_out_msg(paste0(msg1, msg2), log_file)
        return(-1)
    }

    return(0)
}

#' Vertical profile for MeteoRwanda radar data, multiple files.
#'
#' Compute a vertical profile of bird and insect
#' from MeteoRwanda new format \code{ODIM HDF5}
#' and store the outputs into \code{BioDBRadar}.
#' 
#' @param bioradar_dir character, full path to the parent folder containing \code{BioConfigRadar}.
#' @param start_time character, start time to process, format 'yyyy-mm-dd hh:mm:ss'.
#' @param end_time character, end time to process, format 'yyyy-mm-dd hh:mm:ss'.
#' @param radar_id integer, the radar id.
#' @param reverse logical, if \code{TRUE} the data is computed from the last date in backward.
#' 
#' @export

process_rwanda_vps <- function(
    bioradar_dir, start_time, end_time,
    radar_id = 1, reverse = FALSE
){
    config_dir <- file.path(bioradar_dir, 'BioConfigRadar')
    dir_config <- file.path(config_dir, 'config')
    log_file <- get_log_file(bioradar_dir, 'logs_vp', 'vp')

    yaml_file <- file.path(dir_config, 'config_datasets.yaml')
    if(!file.exists(yaml_file)){
        msg <- paste('File does not exist:', yaml_file)
        format_out_msg(msg, log_file)
        return(-1)
    }
    radar_info <- yaml::read_yaml(yaml_file)

    radar_files <- get_data_files_list(
        radar_info$radar$polar, start_time, end_time
    )
    if(is.null(radar_files)){
        msg <- paste(
            'No data found for time from',
            start_time, 'to', end_time
        )
        format_out_msg(msg, log_file)
        return(-1)
    }

    radar_files <- lapply(radar_files, function(f){
        file.path(f$dir, f$files)
    })
    radar_files <- do.call(c, radar_files)
    radar_files <- file.path(
        radar_info$radar$polar$dir, radar_files
    )
    if(reverse){
        radar_files <- rev(radar_files)
    }

    klust <- parallel::makeCluster(5)
    doSNOW::registerDoSNOW(klust)
    `%dopar%` <- foreach::`%dopar%`

    ret <- foreach::foreach(
            jlp = seq_along(radar_files)
        ) %dopar% {
        ret <- try(
            wrapper_rwanda_vp(
                radar_files[jlp], bioradar_dir, radar_id
            ),
            silent = TRUE
        )
        if(inherits(ret, 'try-error')){
            msg1 <- paste('File:', radar_files[jlp], '\n')
            msg2 <- gsub('[\r\n]', '', ret[1])
            format_out_msg(paste0(msg1, msg2), log_file)
            return(-1)
        }
        return(0)
    }

    parallel::stopCluster(klust)

    return(0)
}
