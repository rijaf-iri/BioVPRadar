#' Vertical profile.
#'
#' Get a vertical profile for one datetime.
#' 
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param query named list, user query \code{list(parameter, time, species, radarID)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vp <- function(config_dir, query){
    ret <- get_vp_db(config_dir, query)

    if(ret$status == -1) return(ret)

    json <- get_vp_json(ret$vp, query)
    list(status = 0, data = json)
}

#' Vertical profile time series.
#'
#' Get a vertical profile time series.
#' 
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param query named list, user query \code{list(parameter, startTime, endTime, species, radarID)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vpts <- function(config_dir, query){
    ret <- get_vpts_db(config_dir, query)

    if(ret$status == -1) return(ret)

    vpts <- bioRad::bind_into_vpts(ret$vp)
    vpts <- regularize_vpts(vpts)
    json <- get_vpts_json(vpts, query)
    list(status = 0, data = json)
}

#' Image vertical profile time series.
#'
#' Plot vertical profile time series.
#' 
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param query named list, user query \code{list(parameter, startTime, endTime, species, radarID)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vpts_image <- function(config_dir, query){
    ret <- get_vpts_db(config_dir, query)

    if(ret$status == -1) return(ret)

    vpts <- bioRad::bind_into_vpts(ret$vp)
    vpts <- regularize_vpts(vpts)
    vpts$datetime <- time_utc2time_local(
        vpts$datetime, 'Africa/Kigali'
    )
    vpts$daterange <- time_utc2time_local(
        vpts$daterange, 'Africa/Kigali'
    )

    pngfile <- tempfile()
    grDevices::png(pngfile, width = 900, height = 450)
    plot(
        vpts, quantity = query$parameter,
        xlab = '', ylab = 'Altitude [m]',
        ylim = c(1600, 5020), 
        cex.lab = 1.5, cex.axis = 1.2,
        cex.main = 1.5, font.main = 2
    )
    grDevices::dev.off()

    bin_data <- readBin(pngfile, 'raw', file.info(pngfile)[1, 'size'])
    bin_data <- RCurl::base64Encode(bin_data, 'txt')
    png_base64 <- paste0('data:image/png;base64,', bin_data)
    unlink(pngfile)

    list(status = 0, data = png_base64)
}

#' Vertical and time integration of profiles.
#'
#' Get the vertical and time integration of profiles.
#' 
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param query named list, user query \code{list(parameter, startTime, endTime, species, radarID)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vtip <- function(config_dir, query){
    ret <- get_vpts_db(config_dir, query)

    if(ret$status == -1) return(ret)

    vpts <- bioRad::bind_into_vpts(ret$vp)
    vpts <- regularize_vpts(vpts)
    json <- get_vtip_json(vpts, query)
    list(status = 0, data = json)
}

#' Image vertical and time integration of profiles.
#'
#' Plot the vertical and time integration of profiles.
#' 
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param query named list, user query \code{list(parameter, startTime, endTime, species, radarID)}.
#'
#' @return A named list, \code{list(status, data)}
#' 
#' @export

get_vtip_image <- function(config_dir, query){
    ret <- get_vpts_db(config_dir, query)

    if(ret$status == -1) return(ret)

    vpts <- bioRad::bind_into_vpts(ret$vp)
    vpts <- regularize_vpts(vpts)
    vpi <- bioRad::integrate_profile(vpts)

    # vpi$datetime <- time_utc2time_local(
    #     vpi$datetime, 'Africa/Kigali'
    # )

    pngfile <- tempfile()
    grDevices::png(pngfile, width = 900, height = 450)
    plot(
        vpi, quantity = query$parameter,
        night_shade = TRUE, xlab = 'Time (UTC)',
        cex.lab = 1.5, cex.axis = 1.2,
        cex.main = 1.5, font.main = 2
    )
    graphics::grid()
    grDevices::dev.off()

    bin_data <- readBin(pngfile, 'raw', file.info(pngfile)[1, 'size'])
    bin_data <- RCurl::base64Encode(bin_data, 'txt')
    png_base64 <- paste0('data:image/png;base64,', bin_data)
    unlink(pngfile)

    list(status = 0, data = png_base64)
}

regularize_vpts <- function(vpts){
    vpts <- bioRad::regularize_vpts(vpts)

    ix <- seq_along(vpts$height)

    sunrise <- lapply(ix, function(i){
        fill_na_nearest(vpts$data$sunrise[i, ])
    })
    vpts$data$sunrise <- do.call(rbind, sunrise)
    sunset <- lapply(ix, function(i){
        fill_na_nearest(vpts$data$sunset[i, ])
    })
    vpts$data$sunset <- do.call(rbind, sunset)
    day <- lapply(ix, function(i){
        fill_na_nearest(vpts$data$day[i, ])
    })
    vpts$data$day <- do.call(rbind, day)

    return(vpts)
}
