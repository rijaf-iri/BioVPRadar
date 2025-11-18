
get_log_file <- function(bioradar_dir, log_dir, prefix){
    dir_log <- file.path(
        bioradar_dir, 'BioDataRadar', log_dir
    )
    if(!dir.exists(dir_log)){
        dir.create(
            dir_log, recursive = TRUE,
            showWarnings = FALSE
        )
    }
    date_log <- format(Sys.time(), '%Y%m%d')
    file_log <- paste0(prefix, '_', date_log, '.txt')
    path_log <- file.path(dir_log, file_log)
    return(path_log)
}

format_out_msg <- function(msg, log_file, append = TRUE){
    ret <- c(
        paste('Time:', Sys.time(), '\n'),
        msg, '\n',
        '*********************************\n'
    )
    cat(ret, file = log_file, append = append)
}

assign_lists <- function(inupt_args, init_pars){
    pars_name <- names(init_pars)
    data_name <- names(inupt_args)
    inm <- data_name %in% pars_name
    if(any(inm)){
        for(n in data_name[inm])
            init_pars[[n]] <- inupt_args[[n]]
    }

    return(init_pars)
}

extract_filename_dates <- function(filenames, fileformat){
    expr <- gregexpr('%', fileformat)[[1]]
    len <- rep(2, length(expr))
    ret <- NULL
    if(expr[1] != -1){
        re <- FALSE
        ss <- 1
        se <- nchar(fileformat)
        nl <- length(expr)
        for(i in 1:nl){
            re <- c(re, TRUE, FALSE)
            ss <- c(ss, expr[i], expr[i] + len[i])
            j <- nl - i + 1
            se <- c(expr[j] - 1, expr[j] + len[j] - 1, se)
        }

        res <- lapply(seq_along(re), function(i){
            v <- substr(fileformat, ss[i], se[i])
            if(v == '') v <- NULL
            if(re[i]) v <- NULL
            v
        })

        inul <- sapply(res, is.null)
        if(!all(inul)){
            res <- do.call(c, res[!inul])
            res <- res[!duplicated(res)]
            res <- double_backslash_non_alnum(res)
            pattern <- paste0(res, collapse = '|')
            ret <- gsub(pattern, '', filenames)
        }
    }
    check <- grepl('[^[:digit:]]', ret)
    if(any(check)){
        ret[check] <- NA
    }

    return(ret)
}

double_backslash_non_alnum <- function(strings){
    for(i in seq_along(strings)){
        expr <- gregexpr('[^[:alnum:]]', strings[i])
        ex <- expr[[1]]
        if(ex[1] == -1) next

        chr <- rep('', length(ex))
        for(j in seq_along(chr)){
            chr[j] <- substr(strings[i], ex[j], ex[j])
        }
        chr <- chr[!duplicated(chr)]
        for(v in chr){
            pt0 <- paste0('\\', v)
            pt1 <- paste0('\\', pt0)
            strings[i] <- gsub(pt0, pt1, strings[i])
        }
    }

    return(strings)
}

get_data_file_path <- function(data_info, time_str){
    format_time <- '%Y-%m-%d %H:%M:%S'
    time_req <- as.POSIXct(
        time_str, format = format_time, tz = 'UTC'
    )
    date_dir <- format(time_req, data_info$format_dir)
    data_dir <- file.path(data_info$dir, date_dir)
    if(!dir.exists(data_dir)) return(NULL)
    data_files <- list.files(data_dir, data_info$pattern)
    if(length(data_files) == 0) return(NULL)
    date_files <- extract_filename_dates(
        data_files, data_info$format_file
    )
    if(is.null(data_files)) return(NULL)
    ina <- is.na(date_files)
    if(all(ina)) return(NULL)
    data_files <- data_files[!ina]
    date_files <- date_files[!ina]
    date_files <- as.POSIXct(
        date_files, format = '%Y%m%d%H%M%S', tz = 'UTC'
    )
    it <- which.min(
        abs(difftime(date_files, time_req, units = 'secs'))
    )
    file_path <- file.path(data_dir, data_files[it])

    return(file_path)
}

get_data_dates_dir <- function(data_info){
    dates_dir <- list.dirs(
        data_info$dir, full.names = FALSE, recursive = FALSE
    )
    if(length(dates_dir) == 0) return(NULL)
    tmp_dates <- as.POSIXct(
        dates_dir, format = data_info$format_dir, tz = 'UTC'
    )
    tmp_dates <- tmp_dates[!is.na(tmp_dates)]
    if(length(tmp_dates) == 0) return(NULL)
    tmp_dates <- format(tmp_dates, data_info$format_dir)

    return(tmp_dates)
}

get_data_files_list <- function(data_info, start_time, end_time){
    format_time <- '%Y-%m-%d %H:%M:%S'
    start <- as.POSIXct(
        start_time, format = format_time, tz = 'UTC'
    )
    end <- as.POSIXct(
        end_time, format = format_time, tz = 'UTC'
    )
    start_date <- as.Date(start)
    end_date <- as.Date(end)

    dates_dir <- get_data_dates_dir(data_info)
    if(is.null(dates_dir)) return(NULL)
    dt_dir <- as.Date(dates_dir, data_info$format_dir)
    it <- dt_dir >= start_date & dt_dir <= end_date
    if(!any(it)) return(NULL)
    dates_dir <- dates_dir[it]

    list_out <- lapply(dates_dir, function(d){
        data_dir <- file.path(data_info$dir, d)
        data_files <- list.files(data_dir, data_info$pattern)
        if(length(data_files) == 0) return(NULL)
        date_files = extract_filename_dates(
            data_files, data_info$format_file
        )
        if(length(date_files) == 0) return(NULL)
        ina <- is.na(date_files)
        if(all(ina)) return(NULL)
        data_files <- data_files[!ina]
        date_files <- date_files[!ina]
        date_files <- as.POSIXct(
            date_files, format = '%Y%m%d%H%M%S', tz = 'UTC'
        )
        it <- date_files >= start & date_files <= end
        if(!any(it)) return(NULL)

        list(dir = d, files =  data_files[it])
    })

    inul <- sapply(list_out, is.null)
    if(all(inul)) return(NULL)

    return(list_out[!inul])
}

# from old version, vp stored in csv files
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
