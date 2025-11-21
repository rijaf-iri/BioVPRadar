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
