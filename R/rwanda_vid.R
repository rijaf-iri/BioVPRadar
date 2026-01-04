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

    rwd_wrong_swp <- jsonlite::fromJSON(sweep_file)
    args <- c(pvol_file = radar_file, rwd_wrong_swp)
    pvol <- do.call(rwanda_read_pvol, args)

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
        vp <- fetch_vp(con, time, species, radar_id, 300)
        if(vp$status == -1) return(-1)

        ppi <- do.call(
            bioRad::integrate_to_ppi,
            c(list(pvol = pvol, vp = vp$vp), vid_conf)
        )
        vid_file <- sub('\\*', species, vid_info$format_file)
        vid_file <- format(pvol$datetime, vid_file)
        nc_file <- file.path(vid_dir, vid_file)
        write_vid_to_nc(
            ppi, pvol$datetime, nc_file, vid_info
        )
        rm(ppi)
    }

    updateTimeRangeTable(
        con, 'vidnc_timerange', radar_id, pvol$datetime
    )

    rm(pvol)
    gc()
    return(0)
}

write_vid_to_nc <- function(ppi, datetime, nc_file, vid_info){
    parameters <- list(
        list(parameter = 'VID', units = '#/km2',
             name = 'Vertically integrated density',
             zlim = c(0, 200)),
        list(parameter = 'VIR', units = 'cm2/km2',
             name = 'Vertically integrated reflectivity',
             zlim = c(0, 2000)),
        list(parameter = 'eta_sum', units = 'cm2/km3',
             name = 'Sum of observed linear reflectivities',
             zlim = c(0, 2000)),
        list(parameter = 'eta_sum_expected', units = 'cm2/km3',
             name = 'Sum of expected linear reflectivities',
             zlim = c(0, 2000)),
        list(parameter = 'R', units = '',
             name = 'Spatial adjustment factor',
             zlim = c(0, 5)),
        list(parameter = 'overlap', units = '',
             name = 'Distribution overlap',
             zlim = c(0, 1))
    )

    r_grd <- create_longlat_wgs84(ppi$data['overlap'])
    xy_crd <- sp::coordinates(r_grd)
    x_crd <- sort(unique(xy_crd[, 1]))
    y_crd <- sort(unique(xy_crd[, 2]))
    nx <- length(x_crd)
    ny <- length(y_crd)
    oy <- rev(seq_along(y_crd))

    dx <- ncdf4::ncdim_def(
            vid_info$lon, 'degree_east', x_crd,
            longname = 'Longitude'
        )
    dy <- ncdf4::ncdim_def(
            vid_info$lat, 'degree_north', y_crd,
            longname = 'Latitude'
        )
    dt <- ncdf4::ncdim_def(
            vid_info$time,
            'seconds since 1970-01-01 00:00:00',
            as.numeric(datetime),
            longname = 'Time'
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
    for(j in seq_along(parameters)){
        param <- parameters[[j]]$parameter
        z <- ppi$data@data[[param]]
        z[is.nan(z) | is.infinite(z)] <- NA
        zlim <- parameters[[j]]$zlim
        z[!is.na(z) & z < zlim[1]] <- zlim[1]
        z[!is.na(z) & z > zlim[2]] <- zlim[2]
        z[is.na(z)] <- vid_info$missval
        z <- matrix(z, nx, ny)
        z <- z[, oy]
        dim(z) <- c(dim(z), 1)
        ncdf4::ncvar_put(nc, ncgrd[[j]], z)
        rm(z)
    }
    ncdf4::nc_close(nc)

    rm(r_grd, xy_crd)
    gc()
    return(0)
}

create_longlat_wgs84 <- function(sgdf){
    crs_str <- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
    g_data <- sp::spTransform(sgdf, sp::CRS(crs_str))
    r_grd <- raster::raster(
        raster::extent(g_data),
        ncol = sgdf@grid@cells.dim[1],
        nrow = sgdf@grid@cells.dim[2],
        crs = raster::crs(g_data)
    )
    rm(g_data)
    return(r_grd)
}
