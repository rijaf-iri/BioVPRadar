compute_vid <- function(
        pvol, vp, vid_conf_file,
        vid_output_dir,
        nyquist_velocity = NULL
    )
{
    vid_conf <- jsonlite::fromJSON(vid_conf_file)
    ppi <- do.call(
        bioRad::integrate_to_ppi,
        c(list(pvol = pvol, vp = vp), vid_conf)
    )
    datetime <- pvol$datetime
    datedir <- format(datetime, '%Y-%m-%d')
    vid_dir <- file.path(vid_output_dir, datedir)
    if(!dir.exists(vid_dir))
        dir.create(vid_dir, showWarnings = FALSE, recursive = TRUE)
    vid_time <- format(datetime, '%Y%m%d%H%M%S')
    vid_file <- paste0('vid_', vid_time, '.nc')
    nc_file <- file.path(vid_dir, vid_file)
    write_vid_to_nc(ppi, datetime, nc_file)
    # convert to xarray/zarr
    return(nc_file)
}

write_vid_to_nc <- function(ppi, datetime, nc_file){
    parameters <- list(
        list(parameter = 'VID', name = 'Vertically integrated density', units = '#/km2'),
        list(parameter = 'VIR', name = 'Vertically integrated reflectivity', units = 'cm2/km2'),
        list(parameter = 'eta_sum', name = 'Sum of observed linear reflectivities', units = 'cm2/km3'),
        list(parameter = 'eta_sum_expected', name = 'Sum of expected linear reflectivities', units = 'cm2/km3'),
        list(parameter = 'R', name = 'Spatial adjustment factor', units = ''),
        list(parameter = 'overlap', name = 'Distribution overlap', units = '')
    )
    missval <- -99999.

    params <- sapply(parameters, '[[', 'parameter')
    data <- lapply(params, function(param){
        rc <- convert_ppi_wgs84(ppi, param)
        xy <- sp::coordinates(rc)
        x <- sort(unique(xy[, 1]))
        y <- sort(unique(xy[, 2]))
        z <- raster::as.matrix(rc)
        z <- t(z)[, rev(seq_along(y))]
        z[is.na(z)] <- missval
        t <- as.numeric(datetime)
        dim(z) <- c(dim(z), 1)
        list(x = x, y = y, z = z, t = t)
    })

    dx <- ncdf4::ncdim_def(
            'lon', 'degreeE', data[[1]]$x,
            longname = 'Longitude'
        )
    dy <- ncdf4::ncdim_def(
            'lat', 'degreeN', data[[1]]$y,
            longname = 'Latitude'
        )
    dt <- ncdf4::ncdim_def(
            'time', 'seconds since 1970-01-01 00:00:00',
            data[[1]]$t, longname = 'Time'
        )
    dxyt <- list(dx, dy, dt)

    ncgrd <- lapply(parameters, function(p){
        ncdf4::ncvar_def(
            tolower(p$parameter), p$units, dxyt,
            missval, p$name, 'float', compression = 9
        )
    })

    nc <- ncdf4::nc_create(nc_file, ncgrd)
    for(j in seq_along(ncgrd))
        ncdf4::ncvar_put(nc, ncgrd[[j]], data[[j]]$z)
    ncdf4::nc_close(nc)
}

convert_ppi_wgs84 <- function(ppi, param){
    wgs84 <- sp::CRS(SRS_string = sf::st_crs(4326)$wkt)
    data <- do.call(function(y) ppi$data[y], list(param))
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
    data <- as.data.frame(
        sp::spTransform(
            methods::as(data, 'SpatialPointsDataFrame'),
            wgs84)
        )
    r_data <- raster::rasterize(data[, 2:3], r_data, data[, 1])
    names(r_data) <- param

    zlim <- switch(tolower(param), 
                   'vid' = c(0, 200),
                   'vir' = c(0, 2000),
                   'r' = c(0, 5),
                   'eta_sum' = c(0, 2000),
                   'eta_sum_expected' = c(0, 2000),
                   'overlap' = c(0, 1))
    r_data[r_data < zlim[1]] <- zlim[1]
    r_data[r_data > zlim[2]] <- zlim[2]

    return(r_data)
}
