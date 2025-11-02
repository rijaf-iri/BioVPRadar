#' Vertical profile.
#'
#' Get a vertical profile for one datetime.
#' 
#' @param data_info named list, list containing the vertical profiles dataset information.
#' To be replaced by a connection to postgresql.
#' @param query named list, user query \code{list(parameter, time)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vp <- function(data_info, query){
    vp_file <- get_vp_file_path(data_info, query$time)
    if(is.null(vp_file)){
        msg <- 'No data found'
        return(list(status = -1, message = msg))
    }
    vp <- bioRad::read_vpts(vp_file)
    json <- get_vp_json(vp, query$parameter)
    list(status = 0, data = json)
}

#' Vertical profile time series.
#'
#' Get a vertical profile time series.
#' 
#' @param data_info named list, list containing the vertical profiles dataset information.
#' To be replaced by a connection to postgresql.
#' @param query named list, user query \code{list(parameter, startTime, endTime)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vpts <- function(data_info, query){
    vp_files <- get_vp_files_path(data_info,
                            query$startTime,
                            query$endTime)
    if(is.null(vp_files)){
        msg <- 'No data found'
        return(list(status = -1, message = msg))
    }
    vpts <- bioRad::read_vpts(vp_files)
    vpts <- bioRad::regularize_vpts(vpts)
    json <- get_vpts_json(vpts, query$parameter)
    list(status = 0, data = json)
}

#' Vertical and time integration of profiles.
#'
#' Get the vertical and time integration of profiles.
#' 
#' @param data_info named list, list containing the vertical profiles dataset information.
#' To be replaced by a connection to postgresql.
#' @param query named list, user query \code{list(parameter, startTime, endTime)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vtip <- function(data_info, query){
    vp_files <- get_vp_files_path(data_info,
                            query$startTime,
                            query$endTime)
    if(is.null(vp_files)){
        msg <- 'No data found'
        return(list(status = -1, message = msg))
    }
    vpts <- bioRad::read_vpts(vp_files)
    vpts <- bioRad::regularize_vpts(vpts)
    json <- get_vtip_json(vpts, query$parameter)
    list(status = 0, data = json)
}

get_vp_file_path <- function(data_info, time_str){
    time_format <- '%Y-%m-%d %H:%M:%S'
    datetime <- as.POSIXct(time_str, format = time_format, tz = 'UTC')
    d_time <- format(datetime, data_info$format_dir)
    d_dir <- file.path(data_info$dir, d_time)
    vp_f <- list.files(d_dir, data_info$pattern)
    if(length(vp_f) == 0) return(NULL)
    vp_times <- as.POSIXct(vp_f, format = data_info$format_file, tz = 'UTC')
    diff_t <- abs(difftime(vp_times, datetime, units = 'secs'))
    time <- vp_times[which.min(diff_t)]
    vp_file <- format(time, data_info$format_file)
    file.path(d_dir, vp_file)
}

get_vp_files_path <- function(data_info, start_time, end_time){
    time_format <- '%Y-%m-%d %H:%M:%S'
    start <- as.POSIXct(start_time, format = time_format, tz = 'UTC')
    end <- as.POSIXct(end_time, format = time_format, tz = 'UTC')
    dates_dir <- seq(as.Date(start), as.Date(end), 'day')
    vp_files <- lapply(dates_dir, function(d){
        d_time <- format(d, data_info$format_dir)
        d_dir <- file.path(data_info$dir, d_time)
        vp_f <- list.files(d_dir, data_info$pattern)
        if(length(vp_f) == 0) return(NULL)
        vp_times <- as.POSIXct(vp_f, format = data_info$format_file, tz = 'UTC')
        it <- vp_times >= start & vp_times <= end
        if(!any(it)) return(NULL)
        paste0(d, '/', vp_f[it])
    })
    vp_files <- do.call(c, vp_files)
    if(length(vp_files) == 0) return(NULL)
    file.path(data_info$dir, vp_files)
}
