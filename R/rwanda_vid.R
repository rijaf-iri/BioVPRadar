#' Spatial estimates of VID.
#'
#' Compute the spatial estimates of 
#' vertically integrated density of bird and insect
#' from MeteoRwanda new format \code{ODIM HDF5}
#' and write the outputs to a netCDF file.
#' 
#' @param radar_file character, full path to a single radar polar volume file.
#' The file data format should be \code{ODIM HDF5} or \code{NEXRAD Level 2}.
#' @param bioradar_dir character, full path to the parent folder containing \code{BioConfigRadar}.
#' @param radar_id integer, the radar id.
#'  
#' @export

wrapper_rwanda_vid <- function(
    radar_file, bioradar_dir, radar_id = 1
){
    on.exit(closeDB(con))
    log_file <- get_log_file(
        bioradar_dir, 'logs_vid', 'vid'
    )
    config_dir <- file.path(
        bioradar_dir, 'BioConfigRadar'
    )

    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        msg <- 'Unable to connect to BioDBRadar.'
        format_out_msg(msg, log_file)
        return(-1)
    }

    dir_config <- file.path(config_dir, 'config')
    sweep_file <- file.path(
        dir_config, 'config_rwanda_new_odim.json'
    )
    if(!file.exists(sweep_file)){
        msg <- paste(
            'File does not exist:', sweep_file
        )
        format_out_msg(msg, log_file)
        return(-1)
    }
    vid_file <- file.path(
        dir_config, 'config_vid.yaml'
    )
    if(!file.exists(vid_file)){
        msg <- paste(
            'File does not exist:', vid_file
        )
        format_out_msg(msg, log_file)
        return(-1)
    }
    dataset_file <- file.path(
        dir_config, 'config_datasets.yaml'
    )
    if(!file.exists(dataset_file)){
        msg <- paste(
            'File does not exist:', dataset_file
        )
        format_out_msg(msg, log_file)
        return(-1)
    }

    # -----------------------------------
    cat(paste('Start | process pvol |', basename(radar_file), '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
    # -----------------------------------

    rwd_wrong_swp <- jsonlite::fromJSON(sweep_file)
    args <- c(pvol_file = radar_file, rwd_wrong_swp)
    pvol <- do.call(rwanda_read_pvol, args)

    # -----------------------------------
    cat(paste('Finish | process pvol |', basename(radar_file), '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
    # -----------------------------------

    vid_conf <- yaml::read_yaml(vid_file)
    vid_info <- yaml::read_yaml(dataset_file)
    vid_info <- vid_info$vertical$sevip

    date_dir <- format(pvol$datetime, vid_info$format_dir)
    radar_dir <- paste0('radar_', radar_id)
    vid_dir <- file.path(vid_info$dir, radar_dir, date_dir)
    if(!dir.exists(vid_dir)){
        dir.create(
            vid_dir, recursive = TRUE, showWarnings = FALSE
        )
    }

    time <- format(pvol$datetime, '%Y-%m-%d %H:%M:%S')

    for(species in c('bird', 'insect')){
        # -----------------------------------
        cat(paste('Start | fetch vp_', species, '|', time, '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
        # -----------------------------------

        vp <- fetch_vp(con, time, species, radar_id, 300)

        # -----------------------------------
        cat(paste('Finish | fetch vp_', species, '|', time, '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
        # -----------------------------------

        if(vp$status == -1) return(-1)

        # -----------------------------------
        cat(paste('Start | compute vid_', species, '|', time, '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
        # -----------------------------------

        ppi <- do.call(
            bioRad::integrate_to_ppi,
            c(list(pvol = pvol, vp = vp$vp), vid_conf)
        )

        # -----------------------------------
        cat(paste('Finish | compute vid_', species, '|', time, '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
        # -----------------------------------

        vid_file <- sub('\\*', species, vid_info$format_file)
        vid_file <- format(pvol$datetime, vid_file)
        nc_file <- file.path(vid_dir, vid_file)

        # -----------------------------------
        cat(paste('Start | write nc vid_', species, '|', time, '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
        # -----------------------------------

        write_vid_to_nc(
            ppi, pvol$datetime, nc_file, vid_info
        )

        # -----------------------------------
        cat(paste('Finish | write nc vid_', species, '|', time, '|', Sys.time(), '\n'), file = paste0(log_file, '.test'), append = TRUE)
        # -----------------------------------
    }

    return(0)
}

write_vid_to_nc <- function(ppi, datetime, nc_file, vid_info){
    parameters <- list(
        list(parameter = 'VID', name = 'Vertically integrated density', units = '#/km2'),
        list(parameter = 'VIR', name = 'Vertically integrated reflectivity', units = 'cm2/km2'),
        list(parameter = 'eta_sum', name = 'Sum of observed linear reflectivities', units = 'cm2/km3'),
        list(parameter = 'eta_sum_expected', name = 'Sum of expected linear reflectivities', units = 'cm2/km3'),
        list(parameter = 'R', name = 'Spatial adjustment factor', units = ''),
        list(parameter = 'overlap', name = 'Distribution overlap', units = '')
    )
    params <- sapply(parameters, '[[', 'parameter')
    data <- lapply(params, function(param){
        rc <- convert_ppi_wgs84(ppi, param)
        xy <- sp::coordinates(rc)
        x <- sort(unique(xy[, 1]))
        y <- sort(unique(xy[, 2]))
        z <- raster::as.matrix(rc)
        z <- t(z)[, rev(seq_along(y))]
        z[is.na(z)] <- vid_info$missval
        t <- as.numeric(datetime)
        dim(z) <- c(dim(z), 1)
        list(x = x, y = y, z = z, t = t)
    })

    dx <- ncdf4::ncdim_def(
            vid_info$lon, 'degreeE', data[[1]]$x,
            longname = 'Longitude'
        )
    dy <- ncdf4::ncdim_def(
            vid_info$lat, 'degreeN', data[[1]]$y,
            longname = 'Latitude'
        )
    dt <- ncdf4::ncdim_def(
            vid_info$time, 'seconds since 1970-01-01 00:00:00',
            data[[1]]$t, longname = 'Time'
        )
    dxyt <- list(dx, dy, dt)

    ncgrd <- lapply(parameters, function(p){
        ncdf4::ncvar_def(
            tolower(p$parameter), p$units, dxyt,
            vid_info$missval, p$name,
            'float', compression = 9
        )
    })

    nc <- ncdf4::nc_create(nc_file, ncgrd)
    for(j in seq_along(ncgrd))
        ncdf4::ncvar_put(nc, ncgrd[[j]], data[[j]]$z)
    ncdf4::nc_close(nc)

    return(0)
}

convert_ppi_wgs84 <- function(ppi, param){
    wgs84 <- sp::CRS(SRS_string = sf::st_crs(4326)$wkt)
    data <- do.call(function(y) ppi$data[y], list(param))
    miss <- is.nan(data@data[, param]) |
            is.na(data@data[, param]) |
            is.infinite(data@data[, param])
    data@data[miss, param] <- 0
    bbox <- sp::spTransform(
                sp::SpatialPoints(
                    t(data@bbox),
                    proj4string = data@proj4string
                ),
                wgs84
            )
    r_data <- raster::raster(raster::extent(bbox),
                        ncol = data@grid@cells.dim[1] * .9,
                        nrow = data@grid@cells.dim[2] * .9,
                        crs = sp::CRS(sp::proj4string(bbox))
                      )
    data <- methods::as(data, 'SpatialPointsDataFrame')
    data <- sp::spTransform(data, wgs84)
    data <- as.data.frame(data)
    data[miss, param] <- NA
    na_rm <- if(all(is.na(data[, param]))) FALSE else TRUE
    r_data <- raster::rasterize(
        data[, 2:3], r_data, data[, 1],
        na.rm = na_rm
    )
    names(r_data) <- param

    zlim <- switch(tolower(param), 
                   'vid' = c(0, 200),
                   'vir' = c(0, 2000),
                   'r' = c(0, 5),
                   'eta_sum' = c(0, 2000),
                   'eta_sum_expected' = c(0, 2000),
                   'overlap' = c(0, 1))
    r_data[!is.na(r_data) & r_data < zlim[1]] <- zlim[1]
    r_data[!is.na(r_data) & r_data > zlim[2]] <- zlim[2]

    return(r_data)
}
